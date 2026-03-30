package main

import (
	"fmt"
	"os"

	"github.com/Gentleman-Programming/engram/internal/config"
	"github.com/Gentleman-Programming/engram/internal/store"
)

func cmdConfig(cfg store.Config) {
	if len(os.Args) < 3 {
		printConfigUsage()
		exitFunc(1)
	}

	switch os.Args[2] {
	case "set":
		cmdConfigSet(cfg)
	case "get":
		cmdConfigGet(cfg)
	case "list":
		cmdConfigList(cfg)
	case "profiles":
		cmdConfigProfiles(cfg)
	case "path":
		fmt.Println(config.Path(cfg.DataDir))
	default:
		fmt.Fprintf(os.Stderr, "unknown config subcommand: %s\n\n", os.Args[2])
		printConfigUsage()
		exitFunc(1)
	}
}

// parseConfigProfile extracts --profile <name> from os.Args[3:] and returns
// the profile name and the remaining args (with the flag pair removed).
func parseConfigProfile() (profile string, args []string) {
	for i := 3; i < len(os.Args); i++ {
		if os.Args[i] == "--profile" && i+1 < len(os.Args) {
			profile = os.Args[i+1]
			// Collect remaining args without the --profile pair.
			args = append(args, os.Args[3:i]...)
			args = append(args, os.Args[i+2:]...)
			return profile, args
		}
	}
	return "", os.Args[3:]
}

func cmdConfigSet(cfg store.Config) {
	localProfile, args := parseConfigProfile()
	// Local --profile on the config subcommand takes priority over
	// global --profile parsed in main(). This is important because
	// "config set" without --profile should always write to root.
	profile := localProfile

	if len(args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: engram config set [--profile NAME] <key> <value>")
		exitFunc(1)
	}
	key := args[0]
	value := args[1]

	if profile != "" {
		if err := config.SetWithProfile(cfg.DataDir, profile, key, value); err != nil {
			fatal(err)
		}
		fmt.Printf("set %s = %s  (profile: %s)\n", key, value, profile)
	} else {
		if err := config.Set(cfg.DataDir, key, value); err != nil {
			fatal(err)
		}
		fmt.Printf("set %s = %s\n", key, value)
	}
}

func cmdConfigGet(cfg store.Config) {
	localProfile, args := parseConfigProfile()

	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "usage: engram config get [--profile NAME] <key>")
		exitFunc(1)
	}
	key := args[0]

	info, ok := config.ValidKeys[key]
	if !ok {
		fatal(fmt.Errorf("unknown config key %q. Valid keys: %s",
			key, config.ValidKeyList()))
	}

	// Priority: env > profile > config > default
	if info.EnvVar != "" {
		if v := os.Getenv(info.EnvVar); v != "" {
			fmt.Printf("%s = %s  (env: %s)\n", key, v, info.EnvVar)
			return
		}
	}

	// Resolve effective profile: local --profile > global cfg.Profile > default-profile
	profile := localProfile
	if profile == "" {
		profile = cfg.Profile
	}
	effectiveProfile := config.ResolveProfile(cfg.DataDir, profile)

	v, err := config.GetWithProfile(cfg.DataDir, effectiveProfile, key)
	if err != nil {
		fatal(err)
	}
	if v != "" {
		source := "config"
		if effectiveProfile != "" {
			// Check if the value actually came from the profile.
			pCfg, _ := config.GetProfileConfig(cfg.DataDir, effectiveProfile)
			if pCfg != nil {
				if pv, ok := pCfg[key]; ok && pv != "" {
					source = fmt.Sprintf("profile: %s", effectiveProfile)
				}
			}
		}
		fmt.Printf("%s = %s  (%s)\n", key, v, source)
		return
	}

	if info.Default != "" {
		fmt.Printf("%s = %s  (default)\n", key, info.Default)
		return
	}

	fmt.Printf("%s = (not set)\n", key)
}

func cmdConfigList(cfg store.Config) {
	localProfile, _ := parseConfigProfile()
	// Local --profile > global cfg.Profile > default-profile
	profile := localProfile
	if profile == "" {
		profile = cfg.Profile
	}
	effectiveProfile := config.ResolveProfile(cfg.DataDir, profile)

	cfgMap, err := config.Load(cfg.DataDir)
	if err != nil {
		fatal(err)
	}

	// If listing a specific profile, show its config
	var profileCfg map[string]string
	if effectiveProfile != "" {
		profileCfg, err = config.GetProfileConfig(cfg.DataDir, effectiveProfile)
		if err != nil {
			fatal(err)
		}
		if profileCfg == nil {
			fmt.Fprintf(os.Stderr, "profile %q not found\n", effectiveProfile)
			exitFunc(1)
		}
		fmt.Printf("Profile: %s\n", effectiveProfile)
	}

	for _, key := range config.SortedKeys() {
		info := config.ValidKeys[key]
		var value, source string

		if info.EnvVar != "" {
			if v := os.Getenv(info.EnvVar); v != "" {
				value = v
				source = "env"
			}
		}

		if source == "" && profileCfg != nil {
			if v, ok := profileCfg[key]; ok && v != "" {
				value = v
				source = fmt.Sprintf("profile: %s", effectiveProfile)
			}
		}

		if source == "" {
			if v, ok := cfgMap[key]; ok && v != "" {
				value = v
				source = "config"
			}
		}

		if source == "" && info.Default != "" {
			value = info.Default
			source = "default"
		}

		if source == "" {
			fmt.Printf("  %s = (not set)\n", key)
		} else {
			fmt.Printf("  %s = %s  (%s)\n", key, value, source)
		}
	}
}

func cmdConfigProfiles(cfg store.Config) {
	profiles, err := config.ListProfiles(cfg.DataDir)
	if err != nil {
		fatal(err)
	}

	if len(profiles) == 0 {
		fmt.Println("No profiles configured.")
		fmt.Println()
		fmt.Println("Create one with:")
		fmt.Println("  engram config set --profile <name> database-url <url>")
		return
	}

	// Check default profile
	defaultProfile, _ := config.Get(cfg.DataDir, "default-profile")

	fmt.Printf("Configured profiles:\n")
	for _, name := range profiles {
		marker := ""
		if name == defaultProfile {
			marker = " (default)"
		}
		fmt.Printf("  %s%s\n", name, marker)
	}
}

func printConfigUsage() {
	fmt.Fprintln(os.Stderr, `usage: engram config <subcommand>

Subcommands:
  set [--profile NAME] <key> <value>  Set a configuration value
  get [--profile NAME] <key>          Get a configuration value (shows source)
  list [--profile NAME]               List all configuration with sources
  profiles                            List all configured profiles
  path                                Print config file path

Valid keys:`)
	for _, key := range config.SortedKeys() {
		info := config.ValidKeys[key]
		envHint := ""
		if info.EnvVar != "" {
			envHint = fmt.Sprintf(" (env: %s)", info.EnvVar)
		}
		fmt.Fprintf(os.Stderr, "  %-18s %s%s\n", key, info.Description, envHint)
	}
}
