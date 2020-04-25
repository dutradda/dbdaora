#!/usr/bin/env bats

@test "should print geted person" {
    run coverage run -p docs/src/index/simple_hash.py
    [ "$status" -eq 0 ]
    [ "$output" = "$(cat docs/src/index/simple_hash.output)" ]
}

@test "should print geted persons from service" {
    run coverage run -p docs/src/index/simple_service.py
    [ "$status" -eq 0 ]
    [ "$output" = "$(cat docs/src/index/simple_service.output)" ]
}

@test "should print geted playlist from repository" {
    run coverage run -p docs/src/index/simple_sorted_set.py
    [ "$status" -eq 0 ]
    [ "$output" = "$(cat docs/src/index/simple_sorted_set.output)" ]
}
