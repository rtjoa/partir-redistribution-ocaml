open Core

module Mesh : sig
  type t [@@deriving equal, sexp]

  val of_alist : (string * int) list -> t Or_error.t
  val axis_size : t -> string -> int Or_error.t
  val expect_same_size : t -> string -> string -> unit Or_error.t
  val same_size_exn : t -> string -> string -> bool
end = struct
  type t = int String.Map.t [@@deriving equal, sexp]

  let of_alist = String.Map.of_alist_or_error
  let axis_size = Map.find_or_error
  let same_size_exn t x y = Map.find_exn t x = Map.find_exn t y

  let expect_same_size t x y =
    let%bind.Or_error x_size = axis_size t x in
    let%bind.Or_error y_size = axis_size t y in
    if x_size = y_size then Ok ()
    else
      error_s
        [%message "Different axis sizes" (t : t) (x : string) (y : string)]
end

module Dim_type = struct
  type t = { local_size : int; axes : string list; global_size : int }
  [@@deriving compare, equal, fields, sexp]

  let create ~local_size ~axes ~global_size = { local_size; axes; global_size }

  let top_axis t =
    match t.axes with
    | top_ax :: _ -> Ok top_ax
    | [] -> error_s [%message "No top axis" (t : t)]

  let gather mesh t =
    match t.axes with
    | top_ax :: rest ->
        let%map.Or_error top_ax_size = Mesh.axis_size mesh top_ax in
        {
          local_size = t.local_size * top_ax_size;
          axes = rest;
          global_size = t.global_size;
        }
    | [] -> error_s [%message "No top axis" (t : t)]

  let has_axis t ~axis = List.mem t.axes axis ~equal:String.equal

  let slice t ~mesh ~axis =
    if has_axis t ~axis then
      error_s [%message "Already has axis" (t : t) (axis : string)]
    else
      let%bind.Or_error axis_size = Mesh.axis_size mesh axis in
      if t.local_size % axis_size <> 0 then
        error_s
          [%message
            "Local size not divisible by new axis"
              (mesh : Mesh.t)
              (t : t)
              (axis : string)]
      else
        Ok
          {
            local_size = t.local_size /% axis_size;
            axes = axis :: t.axes;
            global_size = t.global_size;
          }

  let swap_within t ~axis1 ~axis2 =
    if has_axis t ~axis:axis1 && has_axis t ~axis:axis2 then
      Ok
        {
          local_size = t.local_size;
          axes =
            List.map t.axes ~f:(fun axis ->
                if String.equal axis axis1 then axis2
                else if String.equal axis axis2 then axis1
                else axis);
          global_size = t.global_size;
        }
    else
      error_s
        [%message
          "Must contain both to swap within"
            (t : t)
            (axis1 : string)
            (axis2 : string)]

  let top_two_axes t =
    match t.axes with
    | x :: y :: _ -> Ok (x, y)
    | [ _ ] | [] -> error_s [%message "Expected >=2 axes" (t : t)]

  let invariant mesh t =
    let%bind.Or_error axis_sizes =
      List.map ~f:(Mesh.axis_size mesh) t.axes |> Or_error.all
    in
    if List.fold axis_sizes ~init:t.local_size ~f:Int.( * ) <> t.global_size
    then
      error_s
        [%message
          "Product of local and axes sizes must be global size"
            (mesh : Mesh.t)
            (t : t)]
    else Ok ()
end

module Array_type : sig
  type t [@@deriving compare, equal, sexp]

  val of_list : Dim_type.t list -> t
  val to_list : t -> Dim_type.t list
  val gather : Mesh.t -> t -> int -> t Or_error.t
  val slice : Mesh.t -> t -> int -> string -> t Or_error.t
  val get_dim : t -> int -> Dim_type.t Or_error.t
  val has_axis : t -> string -> bool
  val top_axis : t -> int -> string Or_error.t

  val update_dim :
    t -> int -> f:(Dim_type.t -> Dim_type.t Or_error.t) -> t Or_error.t

  val local_size : t -> int
  val invariant : Mesh.t -> t -> unit Or_error.t
end = struct
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
    else update_dim t i ~f:(Dim_type.slice ~mesh ~axis)

  let map_list t = to_list t |> List.map
  let global_type : t -> int list = map_list ~f:Dim_type.global_size
  let local_type : t -> int list = map_list ~f:Dim_type.local_size
  let local_size t : int = List.fold (local_type t) ~init:1 ~f:Int.( * )

  let invariant mesh t =
    to_list t |> List.map ~f:(Dim_type.invariant mesh) |> Or_error.all_unit
end

module Collective = struct
  type t =
    | All_gather of int
    | Dyn_slice of int * string
    | All_to_all of int * int
    (* All_permute components *)
    | Swap_eq_size_tops of int * int
    | Swap_top_for_eq_size_replicated of int * string
    | Swap_within of int * string * string
  [@@deriving equal, sexp]
end

let fully_undistribute (ty : Array_type.t) =
  Array_type.to_list ty
  |> List.concat_mapi ~f:(fun i dim ->
         Dim_type.axes dim |> List.map ~f:(fun _axis -> Collective.All_gather i))

let fully_distribute_to (ty : Array_type.t) =
  Array_type.to_list ty
  |> List.concat_mapi ~f:(fun i dim ->
         Dim_type.axes dim
         |> List.rev_map ~f:(fun axis -> Collective.Dyn_slice (i, axis)))

let perform_one' (mesh : Mesh.t) (collective : Collective.t) (ty : Array_type.t)
    : Array_type.t Or_error.t =
  let open Dim_type in
  match collective with
  | All_gather i -> Array_type.gather mesh ty i
  | Dyn_slice (i, axis) -> Array_type.slice mesh ty i axis
  | All_to_all (i, j) ->
      let%bind.Or_error from_dim = Array_type.get_dim ty i in
      let%bind.Or_error axis = Dim_type.top_axis from_dim in
      let%bind.Or_error ty = Array_type.gather mesh ty i in
      Array_type.slice mesh ty j axis
  | Swap_eq_size_tops (i, j) ->
      let%bind.Or_error i_axis = Array_type.top_axis ty i in
      let%bind.Or_error j_axis = Array_type.top_axis ty j in
      let%bind.Or_error () = Mesh.expect_same_size mesh i_axis j_axis in
      if i = j then Ok ty
      else
        let%bind.Or_error ty = Array_type.gather mesh ty i in
        let%bind.Or_error ty = Array_type.gather mesh ty j in
        let%bind.Or_error ty = Array_type.slice mesh ty i j_axis in
        Array_type.slice mesh ty j i_axis
  | Swap_top_for_eq_size_replicated (i, replicated_axis) ->
      let%bind.Or_error i_axis = Array_type.top_axis ty i in
      let%bind.Or_error () =
        Mesh.expect_same_size mesh i_axis replicated_axis
      in
      let%bind.Or_error ty = Array_type.gather mesh ty i in
      Array_type.slice mesh ty i replicated_axis
  | Swap_within (i, axis1, axis2) ->
      Array_type.update_dim ty i ~f:(Dim_type.swap_within ~axis1 ~axis2)

let perform_one (mesh : Mesh.t) (collective : Collective.t) (ty : Array_type.t)
    : Array_type.t Or_error.t =
  let%bind.Or_error () = Array_type.invariant mesh ty in
  let%bind.Or_error ty = perform_one' mesh collective ty in
  let%map.Or_error () = Array_type.invariant mesh ty in
  ty

let perform (mesh : Mesh.t) (pgrm : Collective.t list) (ty : Array_type.t) :
    Array_type.t Or_error.t =
  List.fold pgrm ~init:(Ok ty) ~f:(fun ty collective ->
      Or_error.bind ty ~f:(perform_one mesh collective))

let perform_with_history (mesh : Mesh.t) (pgrm : Collective.t list)
    (ty : Array_type.t) =
  let%map.Or_error _, history =
    List.fold pgrm
      ~init:(Ok (ty, [ ty ]))
      ~f:(fun ty_and_history collective ->
        let%bind.Or_error ty, history = ty_and_history in
        let%map.Or_error ty' = perform_one mesh collective ty in
        (ty', ty :: history))
  in
  history

module Collective_with_explicit_axes = struct
  type t =
    | All_gather of int * string
    | Dyn_slice of int * string
    | All_to_all of int * int * string
    | Swap_eq_size_tops of int * int * string * string
    | Swap_top_for_eq_size_replicated of int * string * string
    | Swap_within of int * string * string
  [@@deriving compare, equal, sexp]

  let create (collective : Collective.t) src =
    match collective with
    | All_gather i ->
        let%map.Or_error ax_i = Array_type.top_axis src i in
        All_gather (i, ax_i)
    | Dyn_slice (i, x) -> Dyn_slice (i, x) |> Ok
    | All_to_all (i, j) ->
        let%map.Or_error ax_i = Array_type.top_axis src i in
        All_to_all (i, j, ax_i)
    | Swap_eq_size_tops (i, j) ->
        let%bind.Or_error ax_i = Array_type.top_axis src i in
        let%map.Or_error ax_j = Array_type.top_axis src j in
        Swap_eq_size_tops (i, j, ax_i, ax_j)
    | Swap_top_for_eq_size_replicated (i, x_replicated) ->
        let%map.Or_error ax_i = Array_type.top_axis src i in
        Swap_top_for_eq_size_replicated (i, ax_i, x_replicated)
    | Swap_within (i, x, y) -> Swap_within (i, x, y) |> Ok
end

(* This function is nearly the constructive proof of Lemmas 4.6 and 4.7 *)
let fix_adjacent_collectives' mesh src c1 c2 =
  let open Collective in
  let%bind.Or_error c1' = Collective_with_explicit_axes.create c1 src in
  let%bind.Or_error src' = perform_one mesh c1 src in
  let%map.Or_error c2' = Collective_with_explicit_axes.create c2 src' in
  match (c1', c2') with
  (* Flatten peak: /\ ~~> \epsilon or \_/ or -- *)
  | All_gather (i, ax_i), Dyn_slice (j, ax_j) ->
      if i = j && String.equal ax_i ax_j then []
      else if i = j && not (String.equal ax_i ax_j) then
        if Mesh.same_size_exn mesh ax_i ax_j then
          [ Swap_top_for_eq_size_replicated (i, ax_j) ]
        else [ Dyn_slice (i, ax_j); Swap_within (i, ax_j, ax_i); All_gather i ]
      else if i <> j && String.equal ax_i ax_j then [ All_to_all (i, j) ]
      else [ Dyn_slice (j, ax_j); All_gather i ]
  (* Move rising edge later /¯ ~~> _/ *)
  | All_gather (i, ax_i), All_to_all (k, l, ax_k) ->
      if i = k then
        [ Swap_within (i, ax_i, ax_k); All_to_all (i, l); All_gather i ]
      else if i = l then
        if Mesh.same_size_exn mesh ax_i ax_k then
          [ Swap_eq_size_tops (i, k); All_gather k ]
        else [ All_to_all (k, i); Swap_within (i, ax_i, ax_k); All_gather i ]
      else [ All_to_all (k, l); All_gather i ]
  | All_gather (i, ax_i), Swap_eq_size_tops (k, l, ax_k, ax_l) ->
      let k, l, ax_k, ax_z =
        if i = l then (l, k, ax_k, ax_l) else (k, l, ax_k, ax_l)
      in
      if i = k then
        [
          Swap_within (i, ax_i, ax_k);
          Swap_eq_size_tops (i, l);
          Swap_within (i, ax_i, ax_l);
          All_gather i;
        ]
      else [ Swap_eq_size_tops (k, l); All_gather i ]
  | All_gather (i, _), Swap_within (k, ax_k1, ax_k2) ->
      (* These commute even if i=k; all mentioned axes must be distinct *)
      [ c2; c1 ]
  | ( All_gather (i, ax_i),
      Swap_top_for_eq_size_replicated (k, ax_k, ax_replicated) ) ->
      if i = k && String.equal ax_i ax_replicated then
        [ Swap_within (i, ax_i, ax_k); All_gather i ]
      else if i = k && not (String.equal ax_i ax_replicated) then
        [
          Swap_within (i, ax_i, ax_k);
          Swap_top_for_eq_size_replicated (i, ax_replicated);
          Swap_within (i, ax_replicated, ax_i);
          All_gather i;
        ]
      else if i <> k && String.equal ax_i ax_replicated then
        [ Swap_eq_size_tops (i, k); All_gather i ]
      else [ c2; c1 ]
  (* Move falling edge earlier: ¯\ ~~> \_ *)
  | All_to_all (k, l, ax_k), Dyn_slice (i, ax_i) ->
      if i = k then
        [ Dyn_slice (i, ax_i); Swap_within (i, ax_i, ax_k); All_to_all (i, l) ]
      else if i = l then
        [ Dyn_slice (i, ax_i); All_to_all (k, i); Swap_within (i, ax_i, ax_k) ]
      else [ Dyn_slice (i, ax_i); All_to_all (k, l) ]
  | Swap_eq_size_tops (k, l, ax_k, ax_l), Dyn_slice (i, ax_i) ->
      let k, l, ax_k, ax_z =
        if i = l then (l, k, ax_k, ax_l) else (k, l, ax_k, ax_l)
      in
      if i = k then [ Dyn_slice (i, ax_i); Swap_within (i, ax_i, ax_k) ]
      else [ Dyn_slice (i, ax_i); Swap_eq_size_tops (k, l) ]
  | Swap_within (k, ax_k1, ax_k2), Dyn_slice (i, ax_i) -> [ c2; c1 ]
  | Swap_top_for_eq_size_replicated (k, ax_k, ax_replicated), Dyn_slice (i, ax_i)
    ->
      if i = k && String.equal ax_k ax_i then
        [ Dyn_slice (i, ax_replicated); Swap_within (i, ax_replicated, ax_i) ]
      else if i = k && not (String.equal ax_k ax_i) then
        [
          Dyn_slice (i, ax_replicated);
          Swap_within (i, ax_replicated, ax_k);
          Swap_top_for_eq_size_replicated (i, ax_i);
        ]
      else if i <> k && String.equal ax_k ax_i then
        [ Dyn_slice (i, ax_replicated); Swap_eq_size_tops (i, k) ]
      else [ c2; c1 ]
  (* Flat-flat *)
  | ( ( All_to_all _ | Swap_eq_size_tops _ | Swap_top_for_eq_size_replicated _
      | Swap_within _ ),
      ( All_to_all _ | Swap_eq_size_tops _ | Swap_top_for_eq_size_replicated _
      | Swap_within _ ) )
  (* Flat-rising *)
  | ( ( All_to_all _ | Swap_eq_size_tops _ | Swap_top_for_eq_size_replicated _
      | Swap_within _ ),
      All_gather _ )
  (* Falling-flat *)
  | ( Dyn_slice _,
      ( All_to_all _ | Swap_eq_size_tops _ | Swap_top_for_eq_size_replicated _
      | Swap_within _ ) )
  (* Falling-rising *)
  | Dyn_slice _, All_gather _
  (* Rising-rising *)
  | All_gather _, All_gather _
  (* Falling-falling *)
  | Dyn_slice _, Dyn_slice _ ->
      [ c1; c2 ]

let fix_adjacent_collectives mesh src c1 c2 : Collective.t list Or_error.t =
  let%bind.Or_error original_res = perform mesh [ c1; c2 ] src in
  let%map.Or_error cs = fix_adjacent_collectives' mesh src c1 c2 in
  match perform mesh cs src with
  | Error err ->
      raise_s
        [%message
          "Bug: new program errors"
            (err : Error.t)
            (mesh : Mesh.t)
            (src : Array_type.t)
            (c1 : Collective.t)
            (c2 : Collective.t)
            (cs : Collective.t list)]
  | Ok fixed_res ->
      if not (Array_type.equal fixed_res original_res) then
        let c1' = Collective_with_explicit_axes.create c1 src |> ok_exn in
        let src' = perform_one mesh c1 src |> ok_exn in
        let c2' = Collective_with_explicit_axes.create c2 src' |> ok_exn in
        raise_s
          [%message
            "Bug: fix_adjacent_collectives changed semantics"
              (mesh : Mesh.t)
              (src : Array_type.t)
              (c1 : Collective.t)
              (c2 : Collective.t)
              (c1' : Collective_with_explicit_axes.t)
              (c2' : Collective_with_explicit_axes.t)
              (cs : Collective.t list)]
      else cs

let rec step_to_normal_form (mesh : Mesh.t) (src : Array_type.t)
    (pgrm : Collective.t list) : Collective.t list Or_error.t =
  match pgrm with
  | c1 :: c2 :: rest -> (
      let%bind.Or_error fixed = fix_adjacent_collectives mesh src c1 c2 in
      match fixed with
      | c :: cs ->
          let%bind.Or_error src' = perform_one mesh c src in
          let%map.Or_error fixed_rest =
            step_to_normal_form mesh src' (cs @ rest)
          in
          c :: fixed_rest
      | [] -> step_to_normal_form mesh src rest)
  | [ _ ] | [] -> Ok pgrm

let rec to_normal_form (mesh : Mesh.t) (src : Array_type.t)
    (pgrm : Collective.t list) : Collective.t list Or_error.t =
  let%bind.Or_error pgrm' = step_to_normal_form mesh src pgrm in
  let%bind.Or_error original_res = perform mesh pgrm src in
  let%bind.Or_error fixed_res = perform mesh pgrm' src in
  assert (Array_type.equal original_res fixed_res);
  if List.equal Collective.equal pgrm' pgrm then Ok pgrm'
  else to_normal_form mesh src pgrm'

(* "Easy redistribution" as described at the top of Sec. 4.3 *)
let redistribute_easy (src : Array_type.t) (target : Array_type.t) =
  List.append (fully_undistribute src) (fully_distribute_to target)

let redistribute (mesh : Mesh.t) (src : Array_type.t) (target : Array_type.t) :
    Collective.t list Or_error.t =
  to_normal_form mesh src (redistribute_easy src target)

let rec base_offset_dim mesh (dim : Dim_type.t) (i : int String.Map.t) :
    int Or_error.t =
  match dim with
  | { local_size; axes = []; global_size } ->
      if local_size = global_size then Ok 0
      else error_s [%message "Bad dim" (dim : Dim_type.t)]
  | { local_size; axes = axis :: rest; global_size } ->
      let%bind.Or_error axis_size = Mesh.axis_size mesh axis in
      let%bind.Or_error axis_idx = Map.find_or_error i axis in
      let%map.Or_error tl_res =
        base_offset_dim mesh
          (Dim_type.create ~local_size:(local_size * axis_size) ~axes:rest
             ~global_size)
          i
      in
      (local_size * axis_idx) + tl_res

let base_offset mesh (ty : Array_type.t) (i : int String.Map.t) :
    int list Or_error.t =
  Array_type.to_list ty
  |> List.map ~f:(fun dim -> base_offset_dim mesh dim i)
  |> Or_error.all

let%expect_test "base offset" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let ty =
    Array_type.of_list
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

let%expect_test "redistribute easy" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let src =
    Array_type.of_list
      [
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
      ]
  in
  let target =
    Array_type.of_list
      [
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
      ]
  in
  let pgrm = redistribute_easy src target in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect
    {|
    ((All_gather 0) (All_gather 0) (All_gather 1) (All_gather 1) (Dyn_slice 0 y2)
     (Dyn_slice 0 y1) (Dyn_slice 1 x2) (Dyn_slice 1 x1)) |}];
  let res = perform mesh pgrm src |> ok_exn in
  [%test_eq: Array_type.t] res target;
  print_s [%sexp (res : Array_type.t)];
  [%expect
    {|
    ((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
     (1 ((local_size 3) (axes (x1 x2)) (global_size 12)))) |}];
  let path = perform_with_history mesh pgrm src |> ok_exn in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
      (1 ((local_size 6) (axes (x2)) (global_size 12))))
     ((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
      (1 ((local_size 12) (axes ()) (global_size 12))))
     ((0 ((local_size 6) (axes (y2)) (global_size 12)))
      (1 ((local_size 12) (axes ()) (global_size 12))))
     ((0 ((local_size 12) (axes ()) (global_size 12)))
      (1 ((local_size 12) (axes ()) (global_size 12))))
     ((0 ((local_size 12) (axes ()) (global_size 12)))
      (1 ((local_size 6) (axes (y2)) (global_size 12))))
     ((0 ((local_size 12) (axes ()) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 6) (axes (x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))) |}];
  let local_sizes = List.map path ~f:Array_type.local_size in
  print_s [%sexp (local_sizes : int list)];
  [%expect {| (12 24 72 144 72 24 12 6 6) |}]

let%expect_test "redistribute" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let src =
    Array_type.of_list
      [
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
      ]
  in
  let target =
    Array_type.of_list
      [
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
      ]
  in
  let pgrm = redistribute mesh src target |> ok_exn in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect
    {|
    ((Swap_within 1 y1 y2) (Swap_within 0 x1 x2) (Swap_eq_size_tops 0 1)
     (Swap_within 0 x1 y2) (Swap_within 1 x2 y1) (All_to_all 1 0)
     (Swap_within 0 x1 y1) (All_to_all 0 1)) |}];
  let path = perform_with_history mesh pgrm src |> ok_exn in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 1) (axes (x1 y1 y2)) (global_size 12)))
      (1 ((local_size 6) (axes (x2)) (global_size 12))))
     ((0 ((local_size 1) (axes (y1 x1 y2)) (global_size 12)))
      (1 ((local_size 6) (axes (x2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 y2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 x2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 y2)) (global_size 12)))
      (1 ((local_size 2) (axes (x2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (y2 x1)) (global_size 12)))
      (1 ((local_size 2) (axes (x2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (x2 x1)) (global_size 12)))
      (1 ((local_size 2) (axes (y2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y2 y1)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 3) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))) |}];
  let local_sizes = List.map path ~f:Array_type.local_size in
  print_s [%sexp (local_sizes : int list)];
  [%expect {| (6 6 6 6 6 6 6 6 6) |}]

let%expect_test "redistribute 2" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let src =
    Array_type.of_list
      [
        Dim_type.create ~local_size:6 ~axes:[ "x2" ] ~global_size:12;
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
      ]
  in
  let target =
    Array_type.of_list
      [
        Dim_type.create ~local_size:4 ~axes:[ "y1" ] ~global_size:12;
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
      ]
  in
  let pgrm = redistribute mesh src target |> ok_exn in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect
    {|
    ((All_to_all 1 0) (Swap_within 0 x2 y1) (All_to_all 0 1)
     (Swap_within 1 x2 y2) (Swap_top_for_eq_size_replicated 1 x1)) |}];
  let path = perform_with_history mesh pgrm src |> ok_exn in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (y2 x2)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x2 y2)) (global_size 12))))
     ((0 ((local_size 2) (axes (x2 y1)) (global_size 12)))
      (1 ((local_size 6) (axes (y2)) (global_size 12))))
     ((0 ((local_size 2) (axes (y1 x2)) (global_size 12)))
      (1 ((local_size 6) (axes (y2)) (global_size 12))))
     ((0 ((local_size 6) (axes (x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 6) (axes (x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))) |}];
  let local_sizes = List.map path ~f:Array_type.local_size in
  print_s [%sexp (local_sizes : int list)];
  [%expect {| (12 12 12 12 12 12) |}]

let%expect_test "redistribute 3" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let src =
    Array_type.of_list
      [
        Dim_type.create ~local_size:4 ~axes:[ "y1" ] ~global_size:12;
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
      ]
  in
  let target =
    Array_type.of_list
      [
        Dim_type.create ~local_size:6 ~axes:[ "x2" ] ~global_size:12;
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
      ]
  in
  let pgrm = redistribute mesh src target |> ok_exn in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect
    {|
    ((Swap_within 1 x1 x2) (All_to_all 1 0) (Swap_within 0 y1 x2)
     (Swap_top_for_eq_size_replicated 1 y2) (All_to_all 0 1)) |}];
  let path = perform_with_history mesh pgrm src |> ok_exn in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 2) (axes (y1 x2)) (global_size 12)))
      (1 ((local_size 6) (axes (y2)) (global_size 12))))
     ((0 ((local_size 2) (axes (y1 x2)) (global_size 12)))
      (1 ((local_size 6) (axes (x1)) (global_size 12))))
     ((0 ((local_size 2) (axes (x2 y1)) (global_size 12)))
      (1 ((local_size 6) (axes (x1)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x2 x1)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))) |}];
  let local_sizes = List.map path ~f:Array_type.local_size in
  print_s [%sexp (local_sizes : int list)];
  [%expect {| (12 12 12 12 12 12) |}]

let%expect_test "redistribute 4" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 3); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let src =
    Array_type.of_list
      [
        Dim_type.create ~local_size:2 ~axes:[ "x1"; "x2" ] ~global_size:12;
        Dim_type.create ~local_size:2 ~axes:[ "y1"; "y2" ] ~global_size:12;
      ]
  in
  let target =
    Array_type.of_list
      [
        Dim_type.create ~local_size:6 ~axes:[ "y2" ] ~global_size:12;
        Dim_type.create ~local_size:2 ~axes:[ "x2"; "x1" ] ~global_size:12;
      ]
  in
  let pgrm = redistribute mesh src target |> ok_exn in
  print_s [%sexp (pgrm : Collective.t list)];
  [%expect
    {|
    ((Swap_within 1 y1 y2) (Swap_eq_size_tops 0 1) (Swap_within 0 x2 y2)
     (Swap_within 1 x1 y1) (Swap_eq_size_tops 0 1) (All_gather 0)) |}];
  let path = perform_with_history mesh pgrm src |> ok_exn in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 2) (axes (y1 y2)) (global_size 12)))
      (1 ((local_size 2) (axes (x2 x1)) (global_size 12))))
     ((0 ((local_size 2) (axes (x2 y2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 x1)) (global_size 12))))
     ((0 ((local_size 2) (axes (x2 y2)) (global_size 12)))
      (1 ((local_size 2) (axes (x1 y1)) (global_size 12))))
     ((0 ((local_size 2) (axes (y2 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (x1 y1)) (global_size 12))))
     ((0 ((local_size 2) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y2 y1)) (global_size 12))))
     ((0 ((local_size 2) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))
     ((0 ((local_size 2) (axes (x1 x2)) (global_size 12)))
      (1 ((local_size 2) (axes (y1 y2)) (global_size 12))))) |}];
  let local_sizes = List.map path ~f:Array_type.local_size in
  print_s [%sexp (local_sizes : int list)];
  [%expect {| (4 4 4 4 4 4 4) |}]

let%expect_test "normal form" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let src =
    Array_type.of_list
      [
        Dim_type.create ~local_size:4 ~axes:[ "y1" ] ~global_size:12;
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
      ]
  in
  let pgrm =
    [
      Collective.Swap_within (1, "x1", "x2");
      Swap_top_for_eq_size_replicated (1, "y2");
      Collective.Swap_within (1, "x1", "y2");
      Collective.Dyn_slice (0, "x2");
    ]
  in
  let pgrm' = to_normal_form mesh src pgrm |> ok_exn in
  print_s [%sexp (pgrm' : Collective.t list)];
  [%expect
    {|
    ((Dyn_slice 0 y2) (Swap_within 1 x1 x2) (Swap_eq_size_tops 0 1)
     (Swap_within 1 x1 y2)) |}];
  let res = perform mesh pgrm src |> ok_exn in
  let res' = perform mesh pgrm' src |> ok_exn in
  [%test_eq: Array_type.t] res res';
  print_s [%sexp (res : Array_type.t)];
  [%expect
    {|
    ((0 ((local_size 2) (axes (x2 y1)) (global_size 12)))
     (1 ((local_size 3) (axes (x1 y2)) (global_size 12)))) |}];
  print_s [%sexp (res' : Array_type.t)];
  [%expect
    {|
    ((0 ((local_size 2) (axes (x2 y1)) (global_size 12)))
     (1 ((local_size 3) (axes (x1 y2)) (global_size 12)))) |}];
  let path = perform_with_history mesh pgrm' src |> ok_exn in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 2) (axes (x2 y1)) (global_size 12)))
      (1 ((local_size 3) (axes (y2 x1)) (global_size 12))))
     ((0 ((local_size 2) (axes (y2 y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x2 x1)) (global_size 12))))
     ((0 ((local_size 2) (axes (y2 y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))) |}]

let%expect_test "normal form 2" =
  let mesh : Mesh.t =
    Mesh.of_alist [ ("x1", 2); ("x2", 2); ("y1", 3); ("y2", 2) ] |> ok_exn
  in
  let src =
    Array_type.of_list
      [
        Dim_type.create ~local_size:4 ~axes:[ "y1" ] ~global_size:12;
        Dim_type.create ~local_size:3 ~axes:[ "x1"; "x2" ] ~global_size:12;
      ]
  in
  let pgrm =
    [ Collective.All_gather 1; Swap_top_for_eq_size_replicated (1, "x1") ]
  in
  let pgrm' = to_normal_form mesh src pgrm |> ok_exn in
  print_s [%sexp (pgrm' : Collective.t list)];
  [%expect
    {|
    ((Swap_within 1 x1 x2) (All_gather 1)) |}];
  let res = perform mesh pgrm src |> ok_exn in
  let res' = perform mesh pgrm' src |> ok_exn in
  [%test_eq: Array_type.t] res res';
  print_s [%sexp (res : Array_type.t)];
  [%expect
    {|
    ((0 ((local_size 4) (axes (y1)) (global_size 12)))
     (1 ((local_size 6) (axes (x1)) (global_size 12)))) |}];
  print_s [%sexp (res' : Array_type.t)];
  [%expect
    {|
    ((0 ((local_size 4) (axes (y1)) (global_size 12)))
     (1 ((local_size 6) (axes (x1)) (global_size 12)))) |}];
  let path = perform_with_history mesh pgrm' src |> ok_exn in
  print_s [%sexp (path : Array_type.t list)];
  [%expect
    {|
    (((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x2 x1)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))
     ((0 ((local_size 4) (axes (y1)) (global_size 12)))
      (1 ((local_size 3) (axes (x1 x2)) (global_size 12))))) |}]
