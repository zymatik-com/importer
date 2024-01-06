/* SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Zymatik Importer - Import data into a Genobase database.
 * Copyright (C) 2024 Damian Peckett <damian@pecke.tt>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package main

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/urfave/cli/v2"
	"github.com/zymatik-com/importer/internal/importer"
	"github.com/zymatik-com/tools/database"
)

func main() {
	var logger *slog.Logger
	var showProgress bool

	init := func(c *cli.Context) error {
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: (*slog.Level)(c.Generic("log-level").(*logLevelFlag)),
		}))

		showProgress = c.Bool("show-progress")

		return nil
	}

	sharedFlags := []cli.Flag{
		&cli.GenericFlag{
			Name:    "log-level",
			Aliases: []string{"l"},
			Usage:   "Set the log level",
			Value:   fromLogLevel(slog.LevelInfo),
		},
		&cli.BoolFlag{
			Name:    "show-progress",
			Aliases: []string{"p"},
			Usage:   "Show progress bars",
			Value:   true,
		},
		&cli.StringFlag{
			Name:    "database-path",
			Aliases: []string{"db"},
			Usage:   "Set the genobase database path",
			Value:   "zymatik.db",
		},
		&cli.BoolFlag{
			Name:  "no-sync",
			Usage: "Don't sync the database to disk after each operation (unsafe)",
			Value: false,
		},
	}

	app := &cli.App{
		Name:   "importer",
		Usage:  "Prepare a Genobase database from public Human Genomics reference data",
		Flags:  sharedFlags,
		Before: init,
		Commands: []*cli.Command{
			{
				Name:      "add-variants",
				Usage:     "Add dbSNP variants to a Genobase database",
				UsageText: "importer add-variants <dbsnp vcf path>",
				Flags:     sharedFlags,
				Before:    init,
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return fmt.Errorf("missing required dbsnp path argument")
					}

					dbPath := c.String("database-path")
					noSync := c.Bool("no-sync")

					db, err := database.Open(c.Context, logger, dbPath, noSync)
					if err != nil {
						return fmt.Errorf("could not open database: %w", err)
					}
					defer db.Close()

					dbsnpPath := c.Args().First()

					logger.Info("Adding dbSNP variants", "path", dbsnpPath)

					return importer.DBSNP(c.Context, logger, db, dbsnpPath, showProgress)
				},
			},
			{
				Name:      "add-alleles",
				Usage:     "Add gnomAD allele frequencies to a Genobase database",
				UsageText: "importer add-alleles <gnomad vcf path>",
				Flags: append([]cli.Flag{
					&cli.Float64Flag{
						Name:    "minimum-frequency",
						Aliases: []string{"m"},
						Usage:   "The minimum allele frequency to include",
						Value:   0.001, // 0.1% or 1 in 1000.
					},
				}, sharedFlags...),
				Before: init,
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return fmt.Errorf("missing required gnomad path argument")
					}

					dbPath := c.String("database-path")
					noSync := c.Bool("no-sync")

					db, err := database.Open(c.Context, logger, dbPath, noSync)
					if err != nil {
						return fmt.Errorf("could not open database: %w", err)
					}
					defer db.Close()

					gnoMADPath := c.Args().First()
					minimumFrequency := c.Float64("minimum-frequency")

					logger.Info("Adding gnomAD alleles", "path", gnoMADPath, "minimumFrequency", minimumFrequency)

					return importer.GnoMAD(c.Context, logger, db, gnoMADPath, minimumFrequency, showProgress)
				},
			},
			{
				Name:      "add-chain-file",
				Usage:     "Add a liftOver chain file to a Genobase database",
				UsageText: "importer add-chain-file [-f reference] <chain file path>",
				Flags: append([]cli.Flag{
					&cli.StringFlag{
						Name:     "from-reference",
						Aliases:  []string{"f"},
						Usage:    "The reference this chain is from",
						Required: true,
					},
				}, sharedFlags...),
				Before: init,
				Action: func(c *cli.Context) error {
					if c.NArg() != 1 {
						return fmt.Errorf("missing required chain file path argument")
					}

					dbPath := c.String("database-path")
					noSync := c.Bool("no-sync")

					db, err := database.Open(c.Context, logger, dbPath, noSync)
					if err != nil {
						return fmt.Errorf("could not open database: %w", err)
					}
					defer db.Close()

					fromReference := c.String("from-reference")
					chainFilePath := c.Args().First()

					logger.Info("Adding liftOver chain", "fromReference", fromReference, "path", chainFilePath)

					return importer.LiftOverChain(c.Context, logger, db, fromReference, chainFilePath, showProgress)
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		logger.Error("Error running app", "error", err)
		os.Exit(1)
	}
}

type logLevelFlag slog.Level

func fromLogLevel(l slog.Level) *logLevelFlag {
	f := logLevelFlag(l)
	return &f
}

func (f *logLevelFlag) Set(value string) error {
	return (*slog.Level)(f).UnmarshalText([]byte(value))
}

func (f *logLevelFlag) String() string {
	return (*slog.Level)(f).String()
}
