open Core

type t = Dim_type.t Int.Map.t [@@deriving compare, equal, sexp]

let of_list l = List.mapi l ~f:(fun i dim -> (i, dim)) |> Int.Map.of_alist_exn
let to_list t = Map.to_alist ~key_order:`Increasing t |> List.map ~f:snd
let get_dim = Map.find_or_error

let top_axis t i =
  let%bind.Or_error dim = get_dim t i in
  Dim_type.top_axis dim

let update_dim t dim ~f =
  let%bind.Or_error data = Map.find_or_error t dim in
  let%map.Or_error data = f data in
  Map.set ~key:dim ~data t

let gather mesh t i = update_dim t i ~f:(Dim_type.gather mesh)
let has_axis t axis = Map.exists t ~f:(Dim_type.has_axis ~axis)

let slice mesh t i axis =
  if has_axis t axis then
    error_s [%message "Already has axis" (t : t) (axis : string)]
  else update_dim t i ~f:(Dim_type.slice mesh axis)

let map_list t = to_list t |> List.map
let global_type : t -> int list = map_list ~f:Dim_type.global_size
let local_type : t -> int list = map_list ~f:Dim_type.local_size
let local_size t : int = List.fold (local_type t) ~init:1 ~f:Int.( * )

let invariant mesh t =
  to_list t |> List.map ~f:(Dim_type.invariant mesh) |> Or_error.all_unit

let rec base_offset_dim mesh (dim : Dim_type.t) (i : int String.Map.t) :
    int Or_error.t =
  match Dim_type.axes dim with
  | [] ->
      if Dim_type.local_size dim = Dim_type.global_size dim then Ok 0
      else error_s [%message "Bad dim" (dim : Dim_type.t)]
  | axis :: rest ->
      let%bind.Or_error axis_size = Mesh.axis_size mesh axis in
      let%bind.Or_error axis_idx = Map.find_or_error i axis in
      let%map.Or_error tl_res =
        base_offset_dim mesh
          (Dim_type.create
             ~local_size:(Dim_type.local_size dim * axis_size)
             ~axes:rest ~global_size:(Dim_type.global_size dim))
          i
      in
      (Dim_type.local_size dim * axis_idx) + tl_res

let base_offset mesh t (i : int String.Map.t) : int list Or_error.t =
  to_list t
  |> List.map ~f:(fun dim -> base_offset_dim mesh dim i)
  |> Or_error.all

let%expect_test "base offset" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let ty =
    of_list
      [
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
      ]
  in
  let res =
    base_offset mesh ty
      (String.Map.of_alist_exn [ ("x1", 2); ("x2", 10); ("y1", 0); ("y2", 1) ])
    |> ok_exn
  in
  print_s [%sexp (res : int list)];
  [%expect {| (66 6) |}]
