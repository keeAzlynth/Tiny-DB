#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/spdlog.h"

std::once_flag spdlog_init_flag;

void init_spdlog_file() {
  std::call_once(spdlog_init_flag, []() {
    auto max_size  = 1048576 * 5;
    auto max_files = 3;
    auto logger    = spdlog::rotating_logger_mt("Tiny-DB", "logs/tiny_DB.log", max_size, max_files);
    spdlog::set_default_logger(logger);
    spdlog::set_level(spdlog::level::debug);

    spdlog::info("spdlog initialized");
  });
}

void reset_log_level(const std::string& level) {
  auto log_level = spdlog::level::from_str(level);

  spdlog::set_level(log_level);
}
