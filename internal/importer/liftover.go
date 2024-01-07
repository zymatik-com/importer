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
	"log/slog"
	"os"

	"github.com/zymatik-com/tools/compress"
	"github.com/zymatik-com/tools/database"
	"github.com/zymatik-com/tools/liftover"
	"github.com/zymatik-com/tools/liftover/chainfile"
)

// LiftOverChain imports a lift over chain file into the database.
func LiftOverChain(ctx context.Context, logger *slog.Logger, db *database.DB, fromReference, path string, showProgress bool) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("could not open chain file: %w", err)
	}
	defer f.Close()

	dr, err := compress.Decompress(f)
	if err != nil {
		return fmt.Errorf("could not decompress chain file: %w", err)
	}
	defer dr.Close()

	cf, err := chainfile.Read(dr)
	if err != nil {
		return err
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("could not close chain file: %w", err)
	}

	if err := liftover.StoreChainFile(ctx, db, fromReference, cf, showProgress); err != nil {
		return err
	}

	return nil
}
