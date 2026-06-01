package indexer

import (
	"testing"

	"github.com/gateway-fm/chain-indexer/internal/types"
)

func TestClassifyNewToken(t *testing.T) {
	tests := []struct {
		name         string
		isERC721     bool
		metaDecimals int
		wantType     string
		wantDecimals int
	}{
		{
			name:         "erc721 forces type 721 and zero decimals",
			isERC721:     true,
			metaDecimals: 18, // metadata fallback for a contract with no decimals()
			wantType:     types.TokenTypeERC721,
			wantDecimals: 0,
		},
		{
			name:         "erc20 keeps fetched decimals",
			isERC721:     false,
			metaDecimals: 6,
			wantType:     types.TokenTypeERC20,
			wantDecimals: 6,
		},
		{
			name:         "erc20 default decimals preserved",
			isERC721:     false,
			metaDecimals: 18,
			wantType:     types.TokenTypeERC20,
			wantDecimals: 18,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotType, gotDecimals := classifyNewToken(tt.isERC721, tt.metaDecimals)
			if gotType != tt.wantType {
				t.Errorf("token type = %q, want %q", gotType, tt.wantType)
			}
			if gotDecimals != tt.wantDecimals {
				t.Errorf("decimals = %d, want %d", gotDecimals, tt.wantDecimals)
			}
		})
	}
}
