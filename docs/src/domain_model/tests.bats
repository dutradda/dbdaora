#!/usr/bin/env bats

@test "should print domain model entities" {
    run coverage run -p docs/src/domain_model/simple.py
    [ "$status" -eq 0 ]
    [ "$output" = "$(cat docs/src/domain_model/simple.output)" ]
}
