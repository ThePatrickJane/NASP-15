package Settings

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
)

type Settings struct {
	Path                   string
	WalMaxSegments         int `json:"wal_max_segments"`
	MemtableMaxElements    int `json:"memtable_max_elements"`
	LsmMaxLevels           int `json:"lsm_max_levels"`
	LsmMaxElementsPerLevel int `json:"lsm_max_elements_per_level"`
	CacheMaxElements       int `json:"cache_max_elements"`
	TokenBucketMaxTokens   int `json:"token_bucket_max_tokens"`
	TokenBucketInterval    int `json:"token_bucket_interval"`
}

func (settings *Settings) LoadDefault() {
	settings.WalMaxSegments = 3
	settings.MemtableMaxElements = 3
	settings.LsmMaxLevels = 6
	settings.LsmMaxElementsPerLevel = 6
	settings.CacheMaxElements = 4
	settings.TokenBucketMaxTokens = 4
	settings.TokenBucketInterval = 2
}

func (settings *Settings) CheckSettings() {
	if settings.WalMaxSegments == 0 {
		settings.WalMaxSegments = 3
	}
	if settings.MemtableMaxElements == 0 {
		settings.MemtableMaxElements = 3
	}
	if settings.LsmMaxLevels == 0 {
		settings.LsmMaxLevels = 6
	}
	if settings.LsmMaxElementsPerLevel == 0 {
		settings.LsmMaxElementsPerLevel = 6
	}
	if settings.CacheMaxElements == 0 {
		settings.CacheMaxElements = 4
	}
	if settings.TokenBucketMaxTokens == 0 {
		settings.TokenBucketMaxTokens = 4
	}
	if settings.TokenBucketInterval == 0 {
		settings.TokenBucketInterval = 2
	}
}

func (settings *Settings) LoadFromJSON() {
	if _, err := os.Stat(settings.Path); errors.Is(err, os.ErrNotExist) {
		settings.LoadDefault()
	} else {
		data, err := ioutil.ReadFile(settings.Path)
		if err != nil {
			panic(err)
		}
		json.Unmarshal(data, settings)
		settings.CheckSettings()
	}
}
