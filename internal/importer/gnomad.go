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

package importer

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"github.com/brentp/vcfgo"
	"github.com/cheggaaa/pb/v3"
	"github.com/zymatik-com/tools/compress"
	"github.com/zymatik-com/tools/database"
)

var ancestryGroups = []database.AncestryGroup{
	database.AncestryGroupAll,
	database.AncestryGroupAfrican,
	database.AncestryGroupAmish,
	database.AncestryGroupAmerican,
	database.AncestryGroupAshkenazi,
	database.AncestryGroupEastAsian,
	database.AncestryGroupFinnish,
	database.AncestryGroupMiddleEastern,
	database.AncestryGroupEuropean,
	database.AncestryGroupSouthAsian,
}

// GnoMAD imports gnoMAD allele frequency data into the database.
func GnoMAD(ctx context.Context, logger *slog.Logger, db *database.DB, gnoMADPath string, minumumFrequency float64, showProgress bool) error {
	f, err := os.Open(gnoMADPath)
	if err != nil {
		return err
	}
	defer f.Close()

	var dr io.ReadCloser
	if showProgress {
		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("could not get file info: %w", err)
		}

		bar := pb.Full.Start64(fi.Size())
		bar.Set(pb.Bytes, true)
		defer bar.Finish()

		dr, err = compress.Decompress(bar.NewProxyReader(f))
		if err != nil {
			return fmt.Errorf("could not decompress gnoMAD file: %w", err)
		}
	} else {
		dr, err = compress.Decompress(f)
		if err != nil {
			return fmt.Errorf("could not decompress gnoMAD file: %w", err)
		}
	}
	defer dr.Close()

	vcfReader, err := vcfgo.NewReader(dr, false)
	if err != nil {
		return fmt.Errorf("could not create vcf reader: %w", err)
	}

	var alleles []database.Allele
	for {
		variant := vcfReader.Read()
		if variant == nil {
			break
		}

		// Only concerned with high quality variants.
		if variant.Filter != "PASS" {
			continue
		}

		var ids []int64
		if strings.HasPrefix(variant.Id(), "rs") {
			for _, idStr := range strings.Split(variant.Id(), ";") {
				if !strings.HasPrefix(idStr, "rs") {
					continue
				}

				id, err := strconv.ParseInt(strings.TrimPrefix(idStr, "rs"), 10, 64)
				if err != nil {
					logger.Warn("Could not parse variant ID", "id", variant.Id(), "error", err)

					continue
				}

				ids = append(ids, id)
			}
		}

		// Only concerned with variants that have an RSID.
		if len(ids) == 0 {
			continue
		}

		info := variant.Info()

		overallFrequency, err := info.Get("AF")
		if err != nil {
			logger.Warn("Could not get variant frequency", "error", err)
			continue
		}

		// Not concerned with very rare variants.
		if float64(overallFrequency.([]float32)[0]) < minumumFrequency {
			continue
		}

		variantType, err := info.Get("allele_type")
		if err != nil {
			logger.Warn("Could not get variant type", "error", err)
			continue
		}

		// Only concerned with SNVs, and INDELs.
		if strings.ToUpper(variantType.(string)) != "SNV" &&
			strings.ToUpper(variantType.(string)) != "INS" &&
			strings.ToUpper(variantType.(string)) != "DEL" {
			continue
		}

		// Only concerned with biallelic variants.
		if len(variant.Alt()) != 1 {
			continue
		}

		for _, ancestry := range ancestryGroups {
			var frequency float64
			if ancestry == database.AncestryGroupAll {
				frequency = float64(overallFrequency.([]float32)[0])
			} else {
				key := fmt.Sprintf("AF_%s", strings.ToLower(string(ancestry)))

				populationFrequency, err := info.Get(key)
				if err != nil {
					continue
				} else {
					frequency = float64(populationFrequency.([]float32)[0])
				}
			}

			// Conserve space by rounding ancestry group frequencies down to zero where appropriate.
			if frequency > minumumFrequency/100.0 {
				for _, id := range ids {
					alleles = append(alleles, database.Allele{
						ID:        id,
						Reference: variant.Ref(),
						Alternate: variant.Alt()[0],
						Ancestry:  ancestry,
						Frequency: frequency,
					})
				}
			}
		}

		if len(alleles) >= batchSize {
			if err := db.StoreAlleles(ctx, alleles); err != nil {
				return err
			}

			alleles = alleles[:0]
		}
	}

	if len(alleles) > 0 {
		if err := db.StoreAlleles(ctx, alleles); err != nil {
			return err
		}
	}

	return nil
}
