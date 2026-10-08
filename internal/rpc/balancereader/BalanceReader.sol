// SPDX-License-Identifier: MIT
pragma solidity 0.8.20;

/// Reads balanceOf(holder) of one token for many holders in a single eth_call. Never deployed: the indexer and the
/// rig benchmark inject its runtime code with an eth_call state override. ok[i] is false when the token's balanceOf
/// reverted, ran out of its gas allowance, or returned fewer than 32 bytes; out[i] is then 0 and must not be stored.
contract BalanceReader {
    uint256 private constant GAS_PER_READ = 100_000;

    function balances(address token, address[] calldata holders)
        external
        view
        returns (uint256[] memory out, bool[] memory ok)
    {
        out = new uint256[](holders.length);
        ok = new bool[](holders.length);
        for (uint256 i; i < holders.length; ++i) {
            (bool success, bytes memory r) =
                token.staticcall{gas: GAS_PER_READ}(abi.encodeWithSelector(0x70a08231, holders[i]));
            if (success && r.length >= 32) {
                out[i] = abi.decode(r, (uint256));
                ok[i] = true;
            }
        }
    }
}
