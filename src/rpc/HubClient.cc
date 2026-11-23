#include "HubClient.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <Chen/Config/Config.hpp>
#include <Chen/Log/Log.hpp>
#include <Chen/Socket/Address.hpp>
#include <Chen/Util/StringUtil.hpp>
#include <Chen/Util/TimeUtil.hpp>
#include <Chen/Util/TypeUtil.hpp>

namespace bot {

static chen::Logger::ptr logger = LOG_NAME("bot");

static chen::ConfigVar<bool>::ptr g_rpc_enabled = chen::Config::Lookup<bool>("rpc.enabled", true, "enable rpc hub client");
static chen::ConfigVar<std::string>::ptr g_rpc_hub = chen::Config::Lookup<std::string>("rpc.hub_address", "127.0.0.1:9010", "rpc hub address");
static chen::ConfigVar<std::string>::ptr g_rpc_bus_id =
    chen::Config::Lookup<std::string>("rpc.bus_id", "1.1.3.1", "rpc bus id: zone.group.service.instance");
static chen::ConfigVar<uint64_t>::ptr g_rpc_connect_timeout =
    chen::Config::Lookup<uint64_t>("rpc.connect_timeout_ms", (uint64_t)1000, "rpc connect timeout");
static chen::ConfigVar<uint64_t>::ptr g_rpc_reconnect_interval =
    chen::Config::Lookup<uint64_t>("rpc.reconnect_interval_ms", (uint64_t)5000, "rpc reconnect interval");

namespace {

/// App（blog 后端）的 service 编号；func_id = group<<8 | service，group 取自 rpc.bus_id
constexpr uint32_t kAppServiceId = 2;

std::mutex g_mutex;
chen::rpc::RpcClient::ptr g_client;
uint64_t g_last_attempt_ms = 0;
/// 是否有协程正在连接（连接过程会让出协程，不能用 std::mutex 保护）
std::atomic<bool> g_connecting{false};
/// Shutdown 后置 true，阻止在途连接重新发布 g_client
std::atomic<bool> g_stopped{false};

bool ParseBusId(const std::string& value, uint32_t& peer_id, uint32_t& func_id, uint32_t& instance)
{
    auto parts = chen::StringUtil::Split(value, '.', true);
    if (parts.size() != 4) {
        return false;
    }
    uint32_t nums[4] = {0};
    for (size_t i = 0; i < 4; ++i) {
        int32_t v = chen::TypeUtil::Atoi(chen::StringUtil::Trim(parts[i]));
        if (v < 1 || v > 255) {
            return false;
        }
        nums[i] = static_cast<uint32_t>(v);
    }
    peer_id = (nums[0] << 24) | (nums[1] << 16) | (nums[2] << 8) | nums[3];
    func_id = (nums[1] << 8) | nums[2];
    instance = nums[3];
    return true;
}

bool ParseAddress(const std::string& value, chen::Address::ptr& out)
{
    auto parts = chen::StringUtil::Split(value, ':', true);
    if (parts.size() != 2) {
        return false;
    }
    out = chen::IPv4Address::Create(chen::StringUtil::Trim(parts[0]).c_str(), static_cast<uint16_t>(chen::TypeUtil::Atoi(parts[1])));
    return out != nullptr;
}

/// 连接并注册 hub。
/// 注意：绝不能在持有 g_mutex 时调用本函数——connect() 与 registerService()
/// 内部都会 Fiber::YieldToHold()，持 std::mutex 跨 yield 会锁死整个调度器线程。
bool TryConnect()
{
    if (g_connecting.exchange(true)) {
        TRACE(logger) << "rpc hub connect already in progress, skip";
        return false;
    }
    struct ConnectingReset {
        ~ConnectingReset() { g_connecting.store(false); }
    } reset;

    if (g_stopped.load() || !g_rpc_enabled->getValue()) {
        INFO(logger) << "rpc hub disabled or stopped, skip connect";
        return false;
    }

    const std::string hub = g_rpc_hub->getValue();
    if (hub.empty()) {
        WARN(logger) << "rpc.hub_address is empty, skip hub client";
        return false;
    }

    chen::Address::ptr addr;
    if (!ParseAddress(hub, addr)) {
        ERROR(logger) << "invalid rpc.hub_address: " << hub;
        return false;
    }

    uint32_t peer_id = 0;
    uint32_t func_id = 0;
    uint32_t instance = 0;
    const std::string bus_id = g_rpc_bus_id->getValue();
    if (!ParseBusId(bus_id, peer_id, func_id, instance)) {
        ERROR(logger) << "invalid rpc.bus_id: " << bus_id;
        return false;
    }

    auto client = std::make_shared<chen::rpc::RpcClient>();
    if (!client->connect(addr, g_rpc_connect_timeout->getValue())) {
        ERROR(logger) << "connect rpc hub failed: " << hub;
        return false;
    }

    if (!client->registerService(peer_id, func_id, instance, {})) {
        ERROR(logger) << "registerService failed, bus_id=" << bus_id;
        client->close();
        return false;
    }

    bool discard = false;
    {
        std::lock_guard lock(g_mutex);
        if (g_stopped.load()) {
            discard = true;
        } else {
            g_client = client;
        }
    }
    if (discard) {
        INFO(logger) << "rpc hub connected after shutdown, discard, bus_id=" << bus_id;
        client->close();
        return false;
    }

    INFO(logger) << "rpc hub connected, hub=" << hub << " bus_id=" << bus_id << " peer_id=" << peer_id;
    return true;
}

} // namespace

void HubClient::Init()
{
    bool need_connect = false;
    {
        std::lock_guard lock(g_mutex);
        g_stopped.store(false);
        g_last_attempt_ms = chen::TimeUtil::GetCurrentMs();
        need_connect = !(g_client && g_client->isConnected());
    }
    if (need_connect) {
        TryConnect();
    }
}

void HubClient::Shutdown()
{
    chen::rpc::RpcClient::ptr old;
    {
        std::lock_guard lock(g_mutex);
        g_stopped.store(true);
        old.swap(g_client);
    }
    if (old) {
        old->close();
    }
}

void HubClient::Tick()
{
    chen::rpc::RpcClient::ptr old;
    {
        std::lock_guard lock(g_mutex);
        if (!g_rpc_enabled->getValue()) {
            return;
        }
        if (g_client && g_client->isConnected()) {
            return;
        }
        if (g_connecting.load()) {
            return;
        }

        uint64_t now = chen::TimeUtil::GetCurrentMs();
        uint64_t interval = g_rpc_reconnect_interval->getValue();
        if (interval == 0) {
            interval = 5000;
        }
        if (now < g_last_attempt_ms + interval) {
            return;
        }
        g_last_attempt_ms = now;
        old.swap(g_client);
    }

    if (old) {
        old->close();
    }
    TryConnect();
}

chen::rpc::RpcClient::ptr HubClient::Get()
{
    std::lock_guard lock(g_mutex);
    if (!g_client || !g_client->isConnected()) {
        return nullptr;
    }
    return g_client;
}

uint32_t HubClient::AppFuncId()
{
    uint32_t peer_id = 0;
    uint32_t func_id = 0;
    uint32_t instance = 0;
    const std::string bus_id = g_rpc_bus_id->getValue();
    if (!ParseBusId(bus_id, peer_id, func_id, instance)) {
        ERROR(logger) << "invalid rpc.bus_id: " << bus_id << ", fallback app func_id to group 1";
        func_id = 1 << 8;
    }
    // func_id 低 8 位是 service，替换成 App 的 service，group 保持不变
    return (func_id & 0xFF00u) | kAppServiceId;
}

} // namespace bot
