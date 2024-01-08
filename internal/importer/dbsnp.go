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
)

const (
	batchSize = 1000
)

// For the GRCh38 assembly:
var idToChromosome = map[string]string{
	"NC_000001.11": "1",
	"NC_000002.12": "2",
	"NC_000003.12": "3",
	"NC_000004.12": "4",
	"NC_000005.10": "5",
	"NC_000006.12": "6",
	"NC_000007.14": "7",
	"NC_000008.11": "8",
	"NC_000009.12": "9",
	"NC_000010.11": "10",
	"NC_000011.10": "11",
	"NC_000012.12": "12",
	"NC_000013.11": "13",
	"NC_000014.9":  "14",
	"NC_000015.10": "15",
	"NC_000016.10": "16",
	"NC_000017.11": "17",
	"NC_000018.10": "18",
	"NC_000019.10": "19",
	"NC_000020.11": "20",
	"NC_000021.9":  "21",
	"NC_000022.11": "22",
	"NC_000023.11": "X",
	"NC_000024.10": "Y",
	"NC_012920.1":  "MT",
}

// DBSNP imports dbSNP data into the genobase.
func DBSNP(ctx context.Context, logger *slog.Logger, db *genobase.DB, dbSNPPath string, showProgress bool) error {
	f, err := os.Open(dbSNPPath)
	if err != nil {
		return fmt.Errorf("could not open dbSNP file: %w", err)
	}

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
			return fmt.Errorf("could not decompress dbSNP file: %w", err)
		}
	} else {
		dr, err = compress.Decompress(f)
		if err != nil {
			return fmt.Errorf("could not decompress dbSNP file: %w", err)
		}
	}
	defer dr.Close()

	vcfReader, err := vcfgo.NewReader(dr, false)
	if err != nil {
		return fmt.Errorf("could not create vcf reader: %w", err)
	}

	variants := make([]types.Variant, 0, batchSize)
	for {
		variant := vcfReader.Read()
		if variant == nil {
			break
		}

		// Only store "common" variants for now.
		// TODO: use our own allele frequency data if available.
		common, err := variant.Info().Get("COMMON")
		if err != nil {
			return fmt.Errorf("could not get variant commonness: %w", err)
		}
		if !common.(bool) {
			continue
		}

		variantClass, err := variant.Info().Get("VC")
		if err != nil {
			return fmt.Errorf("could not get variant class: %w", err)
		}

		// Do not store multi-nucleotide variants.
		if variantClass.(string) == "MNV" {
			continue
		}

		id, err := strconv.ParseInt(strings.TrimPrefix(variant.Id(), "rs"), 10, 64)
		if err != nil {
			return fmt.Errorf("could not parse variant id: %w", err)
		}

		chromosome, ok := idToChromosome[variant.Chromosome]
		if !ok {
			continue
		}

		parRegion := (chromosome == "X" || chromosome == "Y") && variant.Pos >= 10001 && variant.Pos <= 2781479

		par2Region := (chromosome == "X" && variant.Pos >= 155701383 && variant.Pos <= 156030895) ||
			(chromosome == "Y" && variant.Pos >= 56887903 && variant.Pos <= 57217415)

		// drop pseudo-autosomal copies from Y chromosome.
		if (parRegion || par2Region) && chromosome == "Y" {
			continue
		}

		// Remap pseudo-autosomal regions to a special PAR chromosome
		// (positions will be relative to the X chromosomes).
		if parRegion {
			chromosome = "PAR"
		} else if par2Region {
			chromosome = "PAR2"
		}

		variants = append(variants, types.Variant{
			ID:         id,
			Chromosome: chromosome,
			Position:   int64(variant.Pos),
			Reference:  variant.Ref(),
			Class:      types.VariantClass(variantClass.(string)),
		})

		if len(variants) >= batchSize {
			if err := db.StoreVariants(ctx, variants); err != nil {
				return fmt.Errorf("could not store variants: %w", err)
			}

			variants = variants[:0]
		}
	}

	if len(variants) > 0 {
		if err := db.StoreVariants(ctx, variants); err != nil {
			return fmt.Errorf("could not store variants: %w", err)
		}
	}

	if err := vcfReader.Error(); err != nil {
		return fmt.Errorf("vcf reader error: %w", err)
	}

	return nil
}
