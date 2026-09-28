// A krabka build of fgrosse/kafkactl: all upstream commands plus lab bridge.
package main

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/fgrosse/kafkactl/cmd"
	"github.com/spf13/cobra"
)

func main() {
	root := cmd.New()
	// The WASI broker serves DescribeConfigs v1+, while kafkactl v1.4.0 asks
	// for v0 after fetching topic metadata. Use metadata for this lab listing.
	if topics, _, err := root.Find([]string{"get", "topics"}); err == nil {
		upstream := topics.RunE
		topics.RunE = func(command *cobra.Command, args []string) error {
			context, _ := root.PersistentFlags().GetString("context")
			if context != "krabka-lab" || len(args) != 0 {
				return upstream(command, args)
			}
			config := sarama.NewConfig()
			config.Version = sarama.V1_1_0_0
			config.ClientID = "kafkactl"
			client, err := sarama.NewClient([]string{"127.0.0.1:9092"}, config)
			if err != nil {
				return err
			}
			defer client.Close()
			names, err := client.Topics()
			if err != nil {
				return err
			}
			sort.Strings(names)
			for _, name := range names {
				if !strings.HasPrefix(name, "_") {
					fmt.Fprintln(command.OutOrStdout(), name)
				}
			}
			return nil
		}
	}
	lab := &cobra.Command{Use: "lab", Short: "Connect kafkactl to the krabka browser lab"}
	origin := "https://krabka.io"
	bridge := &cobra.Command{
		Use: "bridge", Short: "Expose the lab's WASI brokers on local Kafka sockets",
		RunE: func(command *cobra.Command, _ []string) error { return runBridge(command.Context(), origin) },
	}
	bridge.Flags().StringVar(&origin, "origin", origin, "exact lab page origin (for local development)")
	lab.AddCommand(bridge)
	root.AddCommand(lab)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		os.Exit(1)
	}
}
