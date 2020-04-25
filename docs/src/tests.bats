#!/usr/bin/env bats


@test "should print typed dict entities" {
    run coverage run -p docs/src/typed_dict.py
    [ "$status" -eq 0 ]
    [ "$output" = "$(cat docs/src/typed_dict.output)" ]
}
