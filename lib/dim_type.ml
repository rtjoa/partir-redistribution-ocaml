open Core

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

let slice mesh axis t =
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

let swap_within axis1 axis2 t =
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

let invariant mesh t =
  let%bind.Or_error axis_sizes =
    List.map ~f:(Mesh.axis_size mesh) t.axes |> Or_error.all
  in
  if List.fold axis_sizes ~init:t.local_size ~f:Int.( * ) <> t.global_size then
    error_s
      [%message
        "Product of local and axes sizes must be global size"
          (mesh : Mesh.t)
          (t : t)]
  else Ok ()
