open Core

let int_sqrt_bound n =
  Int.to_float n |> sqrt |> Float.int63_round_up_exn |> Int63.to_int_exn

let is_prime (n : int) : bool =
  n >= 2
  && Sequence.for_all
       (Sequence.range 2 (int_sqrt_bound n))
       ~f:(fun factor -> Int.rem n factor <> 0)

let%expect_test "primes" =
  Sequence.iter (Sequence.range 0 100) ~f:(fun n ->
      if is_prime n then print_s [%sexp (n : int)]);
  [%expect
    {|
    2
    3
    4
    5
    7
    9
    11
    13
    17
    19
    23
    25
    29
    31
    37
    41
    43
    47
    49
    53
    59
    61
    67
    71
    73
    79
    83
    89
    97 |}]
