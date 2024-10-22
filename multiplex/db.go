package multiplex

import (
	"path/filepath"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/config"
)

// ----------------------------------------------------------------------------
// Multiplex database implementations
// - struct ChainDBContext defines a table name attached to a ChainID
// - struct ChainDB embeds a database instance and adds a ChainID
// - type MultiplexDB maps database instances to ChainID values

// ChainDBContext embeds a [config.DBContext] instance and adds a ChainID
type ChainDBContext struct {
	ChainID string
	config.DBContext
}

// ChainDB embeds a [dbm.DB] instance and adds a ChainID
type ChainDB struct {
	ChainID string
	dbm.DB
}

// MultiplexDB maps ChainIDs to database instances
type MultiplexDB map[string]*ChainDB

// ----------------------------------------------------------------------------
// Providers

// NewMultiplexDB returns multiple databases using the DBBackend and DBDir
// specified in the Config and uses two levels of subfolders for user and chain.
//
// Note that a separate folder is created for every replicated chain and that
// it is organized under a user address parent folder in the `data/` folder.
func NewMultiplexDB(ctx *ChainDBContext) (multiplex MultiplexDB, err error) {
	dbType := dbm.BackendType(ctx.Config.DBBackend)

	// This multiplex maps ChainID to database instances
	multiplex = MultiplexDB{}

	// Storage is located in ChainID subfolders per each user
	for userAddress, chainIds := range ctx.Config.UserChains {
		// Uses one subfolder by user
		dbStorage := filepath.Join(ctx.Config.DBDir(), userAddress)

		for _, chainId := range chainIds {
			chainId, err := NewExtendedChainIDFromLegacy(chainId)
			if err != nil {
				return nil, err
			}

			// .. and one subfolder by ChainID
			dbStorage = filepath.Join(dbStorage, chainId.String())
			chainDb, err := dbm.NewDB(ctx.ID, dbType, dbStorage)
			if err != nil {
				return nil, err
			}

			db := &ChainDB{
				ChainID: chainId.String(),
				DB:      chainDb,
			}

			multiplex[chainId.String()] = db
		}
	}

	return multiplex, nil
}
