#' @title Summarize PID Metadata by Sample
#' @description Summarizes PID metadata to one row per unique \code{sample_id}
#'   with selected metadata fields mapped to standardized column names.
#'
#' @param pid_metadata Either:
#'   \itemize{
#'   \item A metadata data frame from \code{load_pid_files()$metadata}
#'   \item A full list returned by \code{load_pid_files()}, in which case
#'     \code{$metadata} is extracted automatically
#'   }
#' @param conflict_mode Character string. Conflict handling mode:
#'   \itemize{
#'   \item \code{"expand_rows"} (default): return one row per \code{scan_id} for
#'     conflicted \code{sample_id} using observed combinations
#'   \item \code{"stop"}: stop when conflicts are detected
#'   }
#'
#' @return A tibble with one row per \code{sample_id} and columns:
#'   \itemize{
#'   \item \code{sample_id}
#'   \item \code{scan_id}
#'   \item \code{has_conflict}
#'   \item \code{ship}
#'   \item \code{project}
#'   \item \code{station}
#'   \item \code{cruise}
#'   \item \code{tow}
#'   \item \code{net}
#'   \item \code{sample_date}
#'   \item \code{latitude}
#'   \item \code{longitude}
#'   \item \code{min_depth}
#'   \item \code{max_depth}
#'   \item \code{netmesh}
#'   \item \code{netsurf}
#'   \item \code{volume_filtered}
#'   }
#'
#' @details
#' Rules applied by this function:
#' \itemize{
#'   \item If a full PID output list is supplied, \code{$metadata} is used and a
#'     message is emitted.
#'   \item \code{sample_id} must be present and non-missing; otherwise the function stops.
#'   \item If more than one distinct non-missing value is found for any extracted
#'     field within a \code{sample_id}, behavior follows \code{conflict_mode}.
#'   \item Missing extracted keys are returned as \code{NA}.
#'   \item Numeric output columns are coerced to numeric.
#' }
#'
#' @importFrom tibble tibble
#' @importFrom dplyr filter mutate select distinct group_by summarise left_join across n_distinct all_of arrange bind_rows
#' @importFrom tidyr pivot_wider
#' @importFrom purrr reduce map_lgl
#'
#' @export
summarize_pid <- function(pid_metadata, conflict_mode = c("expand_rows", "stop")) {
  conflict_mode <- match.arg(conflict_mode)

  numeric_fields <- c(
    "tow", "net", "latitude", "longitude", "min_depth", "max_depth",
    "netmesh", "netsurf", "volume_filtered"
  )

  key_map <- tibble::tibble(
    section_name = c(
      "Sample", "Sample", "Sample", "parsed_sample", "parsed_sample", "parsed_sample", "parsed_sample",
      "Sample", "Sample", "Sample", "Sample", "Sample", "Sample", "Sample"
    ),
    key = c(
      "Ship", "Scientificprog", "StationId", "cruise", "tow", "net", "sample_date",
      "Latitude", "Longitude", "Zmin", "Zmax", "Netmesh", "Netsurf", "Vol"
    ),
    out = c(
      "ship", "project", "station", "cruise", "tow", "net", "sample_date",
      "latitude", "longitude", "min_depth", "max_depth", "netmesh", "netsurf", "volume_filtered"
    )
  )

  metadata <- if (is.list(pid_metadata) && !is.data.frame(pid_metadata)) {
    if (!"metadata" %in% names(pid_metadata)) {
      stop("When a list is supplied, summarize_pid() requires a `metadata` element.")
    }
    message("summarize_pid(): full PID output detected; using `$metadata`.")
    pid_metadata$metadata
  } else {
    pid_metadata
  }

  if (!is.data.frame(metadata)) {
    stop("summarize_pid() requires a metadata data frame or a full PID output list.")
  }

  required_columns <- c("sample_id", "scan_id", "key", "value")
  missing_columns <- setdiff(required_columns, names(metadata))
  if (length(missing_columns) > 0) {
    stop(
      "summarize_pid() is missing required columns: ",
      paste(missing_columns, collapse = ", ")
    )
  }

  if (any(is.na(metadata$sample_id) | metadata$sample_id == "")) {
    stop("summarize_pid() encountered missing sample_id values.")
  }

  extracted <- metadata |>
    dplyr::filter(.data$key %in% key_map$key) |>
    dplyr::left_join(key_map, by = "key")

  if (all(c("section_name.x", "section_name.y") %in% names(extracted))) {
    extracted <- extracted |>
      dplyr::filter(.data$section_name.x == .data$section_name.y)
  }

  extracted <- extracted |>
    dplyr::transmute(
      sample_id = .data$sample_id,
      scan_id = .data$scan_id,
      field = .data$out,
      value = .data$value
    ) |>
    dplyr::mutate(
      value = trimws(as.character(.data$value)),
      numeric_value = suppressWarnings(as.numeric(.data$value)),
      value_norm = dplyr::case_when(
        .data$field %in% numeric_fields & !is.na(.data$numeric_value) ~ as.character(.data$numeric_value),
        TRUE ~ .data$value
      )
    )

  conflicts <- extracted |>
    dplyr::group_by(.data$sample_id, .data$field) |>
    dplyr::summarise(
      n_unique = dplyr::n_distinct(.data$value_norm[!is.na(.data$value_norm) & .data$value_norm != ""]),
      .groups = "drop"
    ) |>
    dplyr::filter(.data$n_unique > 1)

  if (conflict_mode == "stop" && nrow(conflicts) > 0) {
    conflict_labels <- conflicts |>
      dplyr::mutate(label = paste0(.data$sample_id, " / ", .data$field)) |>
      dplyr::select("label") |>
      dplyr::distinct() |>
      dplyr::pull(.data$label)

    stop(
      "summarize_pid() found conflicting values within sample_id for: ",
      paste(conflict_labels, collapse = "; ")
    )
  }

  summarized_scan <- extracted |>
    dplyr::group_by(.data$sample_id, .data$scan_id, .data$field) |>
    dplyr::summarise(
      value = {
        non_missing <- .data$value_norm[!is.na(.data$value_norm) & .data$value_norm != ""]
        if (length(non_missing) == 0) NA_character_ else non_missing[[1]]
      },
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      names_from = "field",
      values_from = "value"
    )

  summarized_scan <- purrr::reduce(
    key_map$out,
    .init = summarized_scan,
    .f = function(acc, col_name) {
      if (!col_name %in% names(acc)) {
        acc[[col_name]] <- NA_character_
      }
      acc
    }
  ) |>
    dplyr::mutate(
      dplyr::across(dplyr::all_of(numeric_fields), ~ suppressWarnings(as.numeric(.x)))
    )

  conflict_flags <- conflicts |>
    dplyr::distinct(.data$sample_id) |>
    dplyr::mutate(has_conflict = TRUE)

  scan_with_flags <- summarized_scan |>
    dplyr::left_join(conflict_flags, by = "sample_id") |>
    dplyr::mutate(has_conflict = ifelse(is.na(.data$has_conflict), FALSE, .data$has_conflict))

  collapse_per_sample <- function(df) {
    df |>
      dplyr::group_by(.data$sample_id, .data$has_conflict) |>
      dplyr::summarise(
        scan_id = paste(
          sort(unique(.data$scan_id[!is.na(.data$scan_id) & .data$scan_id != ""])),
          collapse = ";"
        ),
        dplyr::across(
          dplyr::all_of(key_map$out),
          ~ {
            non_missing <- .x[!is.na(.x)]
            if (length(non_missing) == 0) NA else non_missing[[1]]
          }
        ),
        .groups = "drop"
      )
  }

  result <- if (conflict_mode == "expand_rows") {
    dplyr::bind_rows(
      collapse_per_sample(dplyr::filter(scan_with_flags, !.data$has_conflict)),
      dplyr::filter(scan_with_flags, .data$has_conflict)
    )
  } else {
    collapse_per_sample(scan_with_flags)
  }

  result <- purrr::reduce(
    c("has_conflict", key_map$out),
    .init = result,
    .f = function(acc, col_name) {
      if (!col_name %in% names(acc)) {
        acc[[col_name]] <- if (col_name == "has_conflict") FALSE else NA_character_
      }
      acc
    }
  )

  output_columns <- c("sample_id", "scan_id", "has_conflict", key_map$out)

  result <- result |>
    dplyr::select(dplyr::all_of(output_columns)) |>
    dplyr::arrange(.data$sample_id, .data$scan_id)

  return(result)
}
