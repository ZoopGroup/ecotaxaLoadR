library(testthat)
library(dplyr)

hex_dir <- testthat::test_path("test-data", "hex_files")


test_that("ingest_hex_file extracts header metadata", {
  test_file <- file.path(hex_dir, "M28_01.hex")
  skip_if_not(file.exists(test_file), "Hex test file missing")

  result <- ingest_hex_file(test_file)

  expect_s3_class(result, "data.frame")
  expect_equal(nrow(result), 1)
  expect_equal(result$ship, "AE")
  expect_equal(result$cruise, "AE2214")
  expect_equal(result$tow, "01")
  expect_equal(result$net_size, 1)
  expect_true(grepl("M28_01.hex", result$header_file_name, fixed = TRUE))
})


test_that("ingest_hex_file warns and fills NA on missing fields", {
  test_file <- file.path(hex_dir, "MOC3_03.hex")
  skip_if_not(file.exists(test_file), "Hex test file missing")

  expect_warning(result <- ingest_hex_file(test_file), "Missing fields")

  expect_equal(result$tow, "03")
  expect_true(is.na(result$net_size))
  expect_false(is.na(result$ship))
})


test_that("load_hex_files batches non-recursively and respects progress flag", {
  skip_if_not(file.exists(file.path(hex_dir, "M28_01.hex")), "Hex test file missing")
  skip_if_not(file.exists(file.path(hex_dir, "MOC3_03.hex")), "Hex test file missing")

  base_dir <- file.path(tempdir(), paste0("hex_tmp_", as.integer(runif(1, 1, 1e6))))
  dir.create(base_dir, recursive = TRUE)

  file.copy(file.path(hex_dir, "M28_01.hex"), file.path(base_dir, "M28_01.hex"))
  file.copy(file.path(hex_dir, "MOC3_03.hex"), file.path(base_dir, "MOC3_03.hex"))

  nested_dir <- file.path(base_dir, "nested")
  dir.create(nested_dir)
  file.copy(file.path(hex_dir, "M28_01.hex"), file.path(nested_dir, "nested.hex"))

  expect_warning(
    expect_message(
      batch <- load_hex_files(directory = base_dir, progress = TRUE),
      "Processing 2 hex files"
    ),
    "Missing fields"
  )

  expect_s3_class(batch, "data.frame")
  expect_equal(nrow(batch), 2)
  expect_equal(sort(unique(batch$file_name)), sort(c("M28_01.hex", "MOC3_03.hex")))

  expect_warning(
    load_hex_files(directory = base_dir, progress = FALSE),
    "Missing fields"
  )

  unlink(base_dir, recursive = TRUE)
})
