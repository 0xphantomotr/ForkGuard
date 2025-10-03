// SPDX-License-Identifier: UNLICENSED
pragma solidity ^0.8.13;

contract Emitter {
    event ValueChanged(uint256 indexed newValue);

    function emitValue(uint256 _newValue) public {
        emit ValueChanged(_newValue);
    }
}