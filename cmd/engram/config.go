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
	case "path":
		fmt.Println(config.Path(cfg.DataDir))
	default:
		fmt.Fprintf(os.Stderr, "unknown config subcommand: %s\n\n", os.Args[2])
		printConfigUsage()
		exitFunc(1)
	}
}

func cmdConfigSet(cfg store.Config) {
	if len(os.Args) < 5 {
		fmt.Fprintln(os.Stderr, "usage: engram config set <key> <value>")
		exitFunc(1)
	}
	key := os.Args[3]
	value := os.Args[4]
	if err := config.Set(cfg.DataDir, key, value); err != nil {
		fatal(err)
	}
	fmt.Printf("set %s = %s\n", key, value)
}

func cmdConfigGet(cfg store.Config) {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: engram config get <key>")
		exitFunc(1)
	}
	key := os.Args[3]

	info, ok := config.ValidKeys[key]
	if !ok {
		fatal(fmt.Errorf("unknown config key %q. Valid keys: %s",
			key, config.ValidKeyList()))
	}

	// Priority: env > config > default
	if info.EnvVar != "" {
		if v := os.Getenv(info.EnvVar); v != "" {
			fmt.Printf("%s = %s  (env: %s)\n", key, v, info.EnvVar)
			return
		}
	}

	v, err := config.Get(cfg.DataDir, key)
	if err != nil {
		fatal(err)
	}
	if v != "" {
		fmt.Printf("%s = %s  (config)\n", key, v)
		return
	}

	if info.Default != "" {
		fmt.Printf("%s = %s  (default)\n", key, info.Default)
		return
	}

	fmt.Printf("%s = (not set)\n", key)
}

func cmdConfigList(cfg store.Config) {
	cfgMap, err := config.Load(cfg.DataDir)
	if err != nil {
		fatal(err)
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

func printConfigUsage() {
	fmt.Fprintln(os.Stderr, `usage: engram config <subcommand>

Subcommands:
  set <key> <value>  Set a configuration value
  get <key>          Get a configuration value (shows source: env/config/default)
  list               List all configuration with sources
  path               Print config file path

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
