#pragma once

#include "CLI11.hpp"

class Flag {
   private:
    CLI::App app;
    std::string config_path_;

   public:
    Flag(const std::string& appname, const std::string& desc,
         const std::string& config_path)
        : app(desc, appname), config_path_(config_path) {
        if (!config_path_.empty()) {
            app.set_config("-c,--config_path", config_path_, "config file path")
                ->configurable(false);
        }
        // Donot throw an error if extra arguments are provided.
        app.allow_extras(true);
    }

    template <typename T>
    CLI::Option* add_option(const std::string& name, T& var,
                            const std::string& desc) {
        return app.add_option(name, var, desc)->capture_default_str();
    }

    CLI::Option* add_flag(const std::string& name, bool& var,
                          const std::string& desc) {
        return app.add_flag(name, var, desc)->capture_default_str();
    }

    template <typename T>
    CLI::Option* add_option(const std::string& name, T& var,
                            const std::string& desc, bool required) {
        return app.add_option(name, var, desc, required)->capture_default_str();
    }

    void deprecate_option(const std::string& option_name,
                          const std::string& replacement = "") {
        CLI::deprecate_option(app, option_name, replacement);
    }

    void retire_option(const std::string& option_name) {
        CLI::retire_option(app, option_name);
    }

    void retire_option(CLI::Option* opt) { CLI::retire_option(app, opt); }

    // Parse the command line arguments and config file(if provided)
    // priority: command line arguments > config file
    // The function can be called multiple times, but the last call will be
    // effective.
    int parser(int argc, const char* const* argv) {
        try {
            app.parse(argc, argv);
            return 0;
        } catch (const CLI::CallForHelp& e) {
            std::cerr << app.help() << std::endl;
            return -1;
        } catch (const CLI::ParseError& e) {
            return app.exit(e);
        };
    }
};
