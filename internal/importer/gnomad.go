/* SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * Zymatik Importer - Import data into a Genobase DB.
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
	"github.com/zymatik-com/genobase"
	"github.com/zymatik-com/genobase/types"
	"github.com/zymatik-com/nucleo/compress"
	"github.com/zymatik-com/nucleo/names"
)

// Ancestry groups which we store allele frequencies for.
var ancestryGroups = []types.AncestryGroup{
	types.AncestryGroupAll,
	types.AncestryGroupAfrican,
	types.AncestryGroupAmish,
	types.AncestryGroupAmerican,
	types.AncestryGroupAshkenazi,
	types.AncestryGroupEastAsian,
	types.AncestryGroupFinnish,
	types.AncestryGroupMiddleEastern,
	types.AncestryGroupEuropean,
	types.AncestryGroupSouthAsian,
}

// Mitochondrial ancestry groups which gnoMAD has allele frequencies for.
// The order of these groups is important as it matches the order of the
// population frequencies in the gnoMAD VCFs.
var mtDNAAncestryGroups = []types.AncestryGroup{
	types.AncestryGroupAfrican,
	types.AncestryGroupAmish,
	types.AncestryGroupAmerican,
	types.AncestryGroupAshkenazi,
	types.AncestryGroupEastAsian,
	types.AncestryGroupFinnish,
	types.AncestryGroupEuropean,
	types.AncestryGroupOther,
	types.AncestryGroupSouthAsian,
	types.AncestryGroupMiddleEastern,
}

// GnoMAD imports gnoMAD allele frequency data into the genobase.
func GnoMAD(ctx context.Context, logger *slog.Logger, db *genobase.DB, gnoMADPath string, minumumFrequency float64, showProgress bool) error {
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

	var alleles []types.Allele
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

		if names.Chromosome(variant.Chromosome) != "MT" {
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

			if len(variant.Alt()) != 1 {
				continue
			}

			for _, ancestry := range ancestryGroups {
				var frequency float64
				if ancestry == types.AncestryGroupAll {
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
						alleles = append(alleles, types.Allele{
							ID:        id,
							Reference: variant.Ref(),
							Alternate: variant.Alt()[0],
							Ancestry:  ancestry,
							Frequency: frequency,
						})
					}
				}
			}
		} else {
			// gnoMADv3 mitochondrial variants are in a totally different format (╯°□°）╯︵ ┻━┻.
			hetFrequency, err := info.Get("AF_het")
			if err != nil {
				logger.Warn("Could not get variant frequency", "error", err)
				continue
			}

			homFrequency, err := info.Get("AF_hom")
			if err != nil {
				logger.Warn("Could not get variant frequency", "error", err)
				continue
			}

			overallFrequency := hetFrequency.(float64) + homFrequency.(float64)

			// Not concerned with very rare variants.
			if overallFrequency < minumumFrequency {
				continue
			}

			// Bit of a horrible hack using the vep field to get the variant type here.
			vep, err := info.Get("vep")
			if err != nil {
				logger.Warn("Could not get variant type", "error", err)

				continue
			}

			// Only concerned with SNVs, and INDELs.
			if !strings.Contains(vep.(string), "insertion") &&
				!strings.Contains(vep.(string), "deletion") &&
				!strings.Contains(vep.(string), "SNV") {
				continue
			}

			if len(variant.Alt()) != 1 {
				continue
			}

			populationHetFrequencies, err := info.Get("pop_AF_het")
			if err != nil {
				logger.Warn("Could not get het variant frequency", "error", err)
				continue
			}

			populationHomFrequencies, err := info.Get("pop_AF_hom")
			if err != nil {
				logger.Warn("Could not get hom variant frequency", "error", err)
				continue
			}

			populationFrequencies := make(map[types.AncestryGroup]float64)
			for i, populationHetFrequencyStr := range strings.Split(populationHetFrequencies.(string), "|") {
				populationHetFrequency, err := strconv.ParseFloat(populationHetFrequencyStr, 64)
				if err != nil {
					logger.Warn("Could not parse variant frequency", "error", err)
					continue
				}

				populationFrequencies[mtDNAAncestryGroups[i]] += populationHetFrequency
			}

			for i, populationHomFrequencyStr := range strings.Split(populationHomFrequencies.(string), "|") {
				populationHomFrequency, err := strconv.ParseFloat(populationHomFrequencyStr, 64)
				if err != nil {
					logger.Warn("Could not parse variant frequency", "error", err)
					continue
				}

				populationFrequencies[mtDNAAncestryGroups[i]] += populationHomFrequency
			}

			for _, ancestry := range ancestryGroups {
				var frequency float64
				if ancestry == types.AncestryGroupAll {
					frequency = overallFrequency
				} else {
					frequency = populationFrequencies[ancestry]
				}

				// Conserve space by rounding ancestry group frequencies down to zero where appropriate.
				if frequency > minumumFrequency/100.0 {
					for _, id := range ids {
						alleles = append(alleles, types.Allele{
							ID:        id,
							Reference: variant.Ref(),
							Alternate: variant.Alt()[0],
							Ancestry:  ancestry,
							Frequency: frequency,
						})
					}
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
