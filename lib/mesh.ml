open Core

type t = int String.Map.t [@@deriving equal, sexp]

let of_alist = String.Map.of_alist_or_error
let axis_size = Map.find_or_error
let same_size_exn t x y = Map.find_exn t x = Map.find_exn t y

let expect_same_size t x y =
  let%bind.Or_error x_size = axis_size t x in
  let%bind.Or_error y_size = axis_size t y in
  if x_size = y_size then Ok ()
  else
    error_s [%message "Different axis sizes" (t : t) (x : string) (y : string)]
