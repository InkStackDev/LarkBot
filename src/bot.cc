#include <Chen/Application.hpp>
#include <Chen/Log/Log.hpp>

static chen::Logger::ptr logger = LOG_NAME("bot");

int main(int argc, char** argv)
{
    try {
        chen::Application app;
        if (app.init(argc, argv)) {
            app.run();
        }
    } catch (const std::exception& e) {
        ERROR(logger) << "Exception: " << e.what();
    }
    return 0;
}