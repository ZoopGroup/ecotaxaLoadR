testthat::test_that("summarize_pid returns one row per sample_id with expected columns", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  summary_tbl <- ecotaxaLoadR::summarize_pid(pid$metadata)

  expected_cols <- c(
    "sample_id", "scan_id", "has_conflict", "ship", "project", "station", "cruise", "tow", "net",
    "sample_date", "latitude", "longitude", "min_depth", "max_depth",
    "netmesh", "netsurf", "volume_filtered"
  )

  testthat::expect_s3_class(summary_tbl, "data.frame")
  testthat::expect_equal(names(summary_tbl), expected_cols)
  testthat::expect_false("section_name" %in% names(summary_tbl))
  testthat::expect_equal(nrow(summary_tbl), dplyr::n_distinct(pid$metadata$sample_id))

  numeric_cols <- c(
    "tow", "net", "latitude", "longitude", "min_depth", "max_depth",
    "netmesh", "netsurf", "volume_filtered"
  )

  testthat::expect_true(all(purrr::map_lgl(summary_tbl[numeric_cols], is.numeric)))
})


testthat::test_that("summarize_pid accepts full load_pid_files output and messages", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  testthat::expect_message(
    summary_from_list <- ecotaxaLoadR::summarize_pid(pid),
    "using `\\$metadata`"
  )

  summary_from_metadata <- ecotaxaLoadR::summarize_pid(pid$metadata)

  testthat::expect_equal(
    dplyr::arrange(summary_from_list, sample_id),
    dplyr::arrange(summary_from_metadata, sample_id)
  )
})


testthat::test_that("summarize_pid errors when list input is missing metadata", {
  bad_input <- list(records = tibble::tibble())

  testthat::expect_error(
    ecotaxaLoadR::summarize_pid(bad_input),
    "requires a `metadata` element"
  )
})


testthat::test_that("summarize_pid errors when sample_id is missing", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  bad_metadata <- pid$metadata
  bad_metadata$sample_id[1] <- NA_character_

  testthat::expect_error(
    ecotaxaLoadR::summarize_pid(bad_metadata),
    "missing sample_id"
  )
})


testthat::test_that("summarize_pid stops on conflicting values within sample_id", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  sample_value <- pid$metadata$sample_id[1]

  conflicting_row <- tibble::tibble(
    scan_id = "synthetic_scan",
    section_name = "Sample",
    key = "Ship",
    value = "DifferentShip",
    sample_id = sample_value
  )

  metadata_conflict <- dplyr::bind_rows(pid$metadata, conflicting_row)

  expanded <- ecotaxaLoadR::summarize_pid(metadata_conflict)

  expanded_sample <- expanded |>
    dplyr::filter(sample_id == sample_value)

  testthat::expect_true(all(expanded_sample$has_conflict))
  testthat::expect_gt(nrow(expanded_sample), 1)
  testthat::expect_true(all(nchar(expanded_sample$scan_id) > 0))
})


testthat::test_that("summarize_pid supports strict stop conflict mode", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  sample_value <- pid$metadata$sample_id[1]

  conflicting_row <- tibble::tibble(
    scan_id = "synthetic_scan",
    section_name = "Sample",
    key = "Ship",
    value = "DifferentShip",
    sample_id = sample_value
  )

  metadata_conflict <- dplyr::bind_rows(pid$metadata, conflicting_row)

  testthat::expect_error(
    ecotaxaLoadR::summarize_pid(metadata_conflict, conflict_mode = "stop"),
    "conflicting values"
  )
})


testthat::test_that("summarize_pid uses Sample/parsed_sample keys when section_name is present", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  sample_value <- pid$metadata$sample_id[1]
  sample_scan <- pid$metadata$scan_id[pid$metadata$sample_id == sample_value][1]

  extra_non_sample <- tibble::tibble(
    scan_id = sample_scan,
    section_name = "NotSample",
    key = c("Zmin", "Zmax"),
    value = c("9999", "9999"),
    sample_id = c(sample_value, sample_value)
  )

  metadata_extended <- dplyr::bind_rows(pid$metadata, extra_non_sample)

  summary_tbl <- ecotaxaLoadR::summarize_pid(metadata_extended)

  original_summary <- ecotaxaLoadR::summarize_pid(pid$metadata)

  testthat::expect_equal(
    dplyr::arrange(summary_tbl, sample_id),
    dplyr::arrange(original_summary, sample_id)
  )
})


testthat::test_that("summarize_pid uses parsed sample_date key", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  expected_dates <- pid$metadata |>
    dplyr::filter(key == "sample_date") |>
    dplyr::distinct(sample_id, sample_date = value)

  summary_tbl <- ecotaxaLoadR::summarize_pid(pid$metadata)

  joined <- dplyr::left_join(
    dplyr::select(summary_tbl, sample_id, sample_date),
    expected_dates,
    by = "sample_id"
  )

  testthat::expect_true(all(joined$sample_date.x == joined$sample_date.y))
})


testthat::test_that("summarize_pid uses parsed cruise key", {
  pid <- ecotaxaLoadR::load_pid_files(
    testthat::test_path("test-data", "pid_files")
  )

  expected_cruise <- pid$metadata |>
    dplyr::filter(key == "cruise") |>
    dplyr::distinct(sample_id, cruise = value)

  summary_tbl <- ecotaxaLoadR::summarize_pid(pid$metadata)

  joined <- dplyr::left_join(
    dplyr::select(summary_tbl, sample_id, cruise),
    expected_cruise,
    by = "sample_id"
  )

  testthat::expect_true(all(joined$cruise.x == joined$cruise.y))
})


testthat::test_that("summarize_pid returns NA for missing extracted keys", {
  synthetic_metadata <- tibble::tibble(
    sample_id = rep("sr9999_m1_n1", 2),
    scan_id = rep("sr9999_m1_n1_d1_a_1", 2),
    key = c("sample_date", "tow"),
    value = c("2024-01-01", "1")
  )

  summary_tbl <- ecotaxaLoadR::summarize_pid(synthetic_metadata)

  testthat::expect_equal(nrow(summary_tbl), 1)
  testthat::expect_true(is.na(summary_tbl$ship))
  testthat::expect_true(is.na(summary_tbl$project))
  testthat::expect_true(is.na(summary_tbl$station))
  testthat::expect_true(is.na(summary_tbl$cruise))
  testthat::expect_equal(summary_tbl$sample_date, "2024-01-01")
  testthat::expect_equal(summary_tbl$tow, 1)
})


testthat::test_that("summarize_pid treats numeric-equivalent depth values as non-conflicting", {
  synthetic_metadata <- tibble::tibble(
    sample_id = c("sr2407_m2_n4", "sr2407_m2_n4", "sr2407_m2_n4", "sr2407_m2_n4"),
    scan_id = c("scan_a", "scan_b", "scan_a", "scan_b"),
    section_name = c("Sample", "Sample", "Sample", "Sample"),
    key = c("Zmin", "Zmin", "Zmax", "Zmax"),
    value = c("10", "10.0", "20", "20.00")
  )

  summary_tbl <- ecotaxaLoadR::summarize_pid(synthetic_metadata)

  testthat::expect_equal(nrow(summary_tbl), 1)
  testthat::expect_equal(summary_tbl$min_depth, 10)
  testthat::expect_equal(summary_tbl$max_depth, 20)
})
