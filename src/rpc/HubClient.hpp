/**
 * @file HubClient.hpp
 * @brief RPC Hub 客户端（连接 InkStackSearchServer，向 App 转发数据）
 * @author Christins
 * @date 2026-11-23
 * @copyright Apache 2.0
 */
#pragma once

#include <cstdint>

#include <Chen/Rpc/RpcClient.hpp>

namespace bot {

/**
 * @brief 进程级 Hub 客户端
 * @details 启动时连接 hub（rpc.hub_address），注册本 Bot 的 bus 身份；
 *          App 通过 GROUPID 路由接收 webhook 数据。断线后由 Tick() 按间隔重连。
 */
class HubClient {
public:
    /// 连接并注册（在 onServerReady 中调用）
    static void Init();

    /// 关闭连接（onUnload / onDeactivate）
    static void Shutdown();

    /// 周期重连（onTick）
    static void Tick();

    /// 已连接时返回客户端，否则返回空
    static chen::rpc::RpcClient::ptr Get();

    /// App 的 func_id（group<<8 | service），用于 GROUPID 路由
    static uint32_t AppFuncId();
};

} // namespace bot
