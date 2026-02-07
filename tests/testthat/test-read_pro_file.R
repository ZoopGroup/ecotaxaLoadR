testthat::test_that("load_pro_files works with test directory containing PRO files", {
  # Test with directory containing SIO PRO files
  test_dir <- testthat::test_path("test-data", "sio_pro_files")

  testthat::expect_true(
    dir.exists(test_dir),
    info = "Test directory should exist"
  )

  result <- ecotaxaLoadR::load_pro_files(test_dir)

  # Check that result is a data frame
  testthat::expect_s3_class(result, "data.frame")

  # Check that we processed some rows
  testthat::expect_gt(nrow(result), 0)

  # Check for required attributes
  testthat::expect_true(
    "failed_files" %in% names(attributes(result)),
    info = "Should have failed_files attribute"
  )
  testthat::expect_true(
    "error_messages" %in% names(attributes(result)),
    info = "Should have error_messages attribute"
  )
  testthat::expect_true(
    "processing_summary" %in% names(attributes(result)),
    info = "Should have processing_summary attribute"
  )

  # Expected columns should be present
  expected_cols <- c("datetime_gmt", "datetime_local", "timezone", "file_name")
  testthat::expect_true(all(expected_cols %in% names(result)))
})

testthat::test_that("load_pro_files works with daynight parameter", {
  # Test with daynight enabled
  test_dir <- testthat::test_path("test-data", "sio_pro_files")

  testthat::expect_true(
    dir.exists(test_dir),
    info = "Test directory should exist"
  )

  # Test with daynight = TRUE
  result_daynight <- ecotaxaLoadR::load_pro_files(test_dir, daynight = TRUE)
  
  # Check that result is a data frame
  testthat::expect_s3_class(result_daynight, "data.frame")
  
  # Check that we processed some rows
  testthat::expect_gt(nrow(result_daynight), 0)
  
  # Test with daynight = FALSE (default)
  result_no_daynight <- ecotaxaLoadR::load_pro_files(test_dir, daynight = FALSE)
  
  # Check that result is a data frame
  testthat::expect_s3_class(result_no_daynight, "data.frame")
  
  # Both should process the same number of rows
  testthat::expect_equal(nrow(result_daynight), nrow(result_no_daynight))
})

testthat::test_that("load_pro_files handles empty directory", {
  # Create a temporary directory with no PRO files
  temp_dir <- tempdir()
  empty_dir <- file.path(temp_dir, "empty_test_dir")
  dir.create(empty_dir, showWarnings = FALSE)

  testthat::expect_error(
    ecotaxaLoadR::load_pro_files(empty_dir),
    "DNF PRO files in this directory"
  )

  # Clean up
  unlink(empty_dir, recursive = TRUE)
})

testthat::test_that("ingest_pro_file works with MOC8 PRO file", {
  # Test with MOC8 file
  pro_file <- testthat::test_path("test-data", "sio_pro_files", "MOC8_08A.PRO")

  testthat::expect_true(file.exists(pro_file))

  result <- ecotaxaLoadR::ingest_pro_file(pro_file)

  # Check that result is a data frame
  testthat::expect_s3_class(result, "data.frame")

  # Check that result has rows and columns
  testthat::expect_gt(nrow(result), 0)
  testthat::expect_gt(ncol(result), 0)

  # Check for expected columns that should be added by ingest_pro_file using purrr::map_lgl
  expected_added_cols <- c(
    "datetime_gmt",
    "datetime_local",
    "timezone",
    "file_name"
  )
  cols_present <- purrr::map_lgl(expected_added_cols, ~ .x %in% names(result))
  purrr::walk2(
    expected_added_cols,
    cols_present,
    ~ {
      testthat::expect_true(.y, info = paste("Should have", .x, "column"))
    }
  )

  # Check for original PRO file columns
  expected_pro_cols <- c("time", "pres", "temp", "sal")
  present_cols <- intersect(expected_pro_cols, names(result))
  testthat::expect_gt(length(present_cols), 0)

  # Check that datetime columns are POSIXct
  testthat::expect_s3_class(result$datetime_gmt, "POSIXct")
  testthat::expect_s3_class(result$datetime_local, "POSIXct")
})

testthat::test_that("ingest_pro_file works with MOC5 PRO file", {
  # Test with MOC5 file
  pro_file <- testthat::test_path("test-data", "sio_pro_files", "MOC5_05A.PRO")

  testthat::expect_true(file.exists(pro_file))

  result <- ecotaxaLoadR::ingest_pro_file(pro_file)

  # Check that result is a data frame
  testthat::expect_s3_class(result, "data.frame")

  # Check that result has rows and columns
  testthat::expect_gt(nrow(result), 0)
  testthat::expect_gt(ncol(result), 0)

  # Check for expected added columns using purrr::walk
  expected_added_cols <- c(
    "datetime_gmt",
    "datetime_local",
    "timezone",
    "file_name"
  )
  purrr::walk(
    expected_added_cols,
    ~ {
      testthat::expect_true(
        .x %in% names(result),
        info = paste("Should have", .x, "column")
      )
    }
  )

  # Check that numeric columns are actually numeric using purrr::walk
  numeric_cols <- c("time", "pres", "temp", "sal")
  intersected_cols <- intersect(numeric_cols, names(result))
  purrr::walk(
    intersected_cols,
    ~ {
      testthat::expect_true(
        is.numeric(result[[.x]]),
        info = paste("Column", .x, "should be numeric")
      )
    }
  )
})

testthat::test_that("ingest_pro_file daynight parameter works correctly", {
  # Test with MOC5 file and daynight annotation
  pro_file <- testthat::test_path("test-data", "sio_pro_files", "MOC5_05A.PRO")

  testthat::expect_true(file.exists(pro_file))

  # Test with daynight = TRUE
  result_with_daynight <- ecotaxaLoadR::ingest_pro_file(pro_file, daynight = TRUE)
  
  # Test with daynight = FALSE (default)
  result_without_daynight <- ecotaxaLoadR::ingest_pro_file(pro_file, daynight = FALSE)

  # Both should be data frames
  testthat::expect_s3_class(result_with_daynight, "data.frame")
  testthat::expect_s3_class(result_without_daynight, "data.frame")

  # Both should have the same number of rows
  testthat::expect_equal(nrow(result_with_daynight), nrow(result_without_daynight))

  # Test with default parameter (should be FALSE)
  result_default <- ecotaxaLoadR::ingest_pro_file(pro_file)
  testthat::expect_s3_class(result_default, "data.frame")
  testthat::expect_equal(nrow(result_default), nrow(result_without_daynight))
})

testthat::test_that("ingest_pro_file fails with non-existent file", {
  non_existent_file <- testthat::test_path(
    "test-data",
    "sio_pro_files",
    "nonexistent.PRO"
  )

  testthat::expect_error(
    ecotaxaLoadR::ingest_pro_file(non_existent_file),
    info = "Should error with non-existent file"
  )
})

testthat::test_that("ingest_pro_file handles invalid file path", {
  invalid_paths <- c("", NULL)
  purrr::walk(
    invalid_paths,
    ~ {
      testthat::expect_error(
        ecotaxaLoadR::ingest_pro_file(.x),
        info = paste("Should error with invalid path:", deparse(.x))
      )
    }
  )
})

testthat::test_that("ingest_pro_file returns consistent structure across files", {
  pro_files <- c(
    moc8 = testthat::test_path("test-data", "sio_pro_files", "MOC8_08A.PRO"),
    moc5 = testthat::test_path("test-data", "sio_pro_files", "MOC5_05A.PRO")
  )

  results <- purrr::map(pro_files, ecotaxaLoadR::ingest_pro_file)

  # Both should be data frames using purrr::map_lgl
  are_dataframes <- purrr::map_lgl(results, ~ inherits(.x, "data.frame"))
  testthat::expect_true(all(are_dataframes))

  # Both should have data using purrr::map_int
  row_counts <- purrr::map_int(results, nrow)
  col_counts <- purrr::map_int(results, ncol)
  testthat::expect_true(all(row_counts > 0))
  testthat::expect_true(all(col_counts > 0))

  # Both should have the same added columns using purrr::map and purrr::every
  added_cols <- c(
    "datetime_gmt",
    "datetime_local",
    "timezone",
    "file_name"
  )
  has_all_cols <- purrr::map(results, function(result_df) {
    purrr::every(added_cols, function(col) col %in% names(result_df))
  })
  testthat::expect_true(all(unlist(has_all_cols)))
})

testthat::test_that("ingest_pro_file preserves data integrity", {
  pro_file <- testthat::test_path("test-data", "sio_pro_files", "MOC5_05A.PRO")

  result <- ecotaxaLoadR::ingest_pro_file(pro_file)

  # Check that time column is monotonic (if present)
  if ("time" %in% names(result) && nrow(result) > 1) {
    time_diffs <- diff(result$time)
    # Time should generally be increasing (allowing for small variations)
    testthat::expect_true(all(time_diffs >= -0.01))
  }

  # Check that pressure values are reasonable (if present) using purrr::map_lgl
  if ("pres" %in% names(result)) {
    pres_checks <- list(
      non_negative = all(result$pres >= 0, na.rm = TRUE),
      reasonable = all(result$pres < 10000, na.rm = TRUE)
    )
    testthat::expect_true(pres_checks$non_negative)
    testthat::expect_true(pres_checks$reasonable)
  }

  # Check that temperature values are reasonable (if present)
  if ("temp" %in% names(result)) {
    testthat::expect_true(all(
      result$temp > -5 & result$temp < 50,
      na.rm = TRUE
    ))
  }

  # Check datetime and metadata columns using purrr::map
  datetime_checks <- purrr::map(
    list(
      gmt_no_na = all(!is.na(result$datetime_gmt)),
      local_no_na = all(!is.na(result$datetime_local)),
      timezone_present = "timezone" %in% names(result),
      filename_consistent = length(unique(result$file_name)) == 1
    ),
    identity
  )

  testthat::expect_true(datetime_checks$gmt_no_na)
  testthat::expect_true(datetime_checks$local_no_na)
  testthat::expect_true(datetime_checks$timezone_present)
  testthat::expect_true(datetime_checks$filename_consistent)
})

testthat::test_that("load_pro_files processes both test files successfully", {
  test_dir <- testthat::test_path("test-data", "sio_pro_files")
  
  result <- ecotaxaLoadR::load_pro_files(test_dir)

  # Check that no files failed
  failed_files <- attr(result, "failed_files")
  testthat::expect_equal(length(failed_files), 0)

  # Check processing summary using purrr::map_lgl
  summary_info <- attr(result, "processing_summary")
  testthat::expect_type(summary_info, "list")

  expected_summary_fields <- c("total_files", "successful_files")
  summary_checks <- purrr::map_lgl(
    expected_summary_fields,
    ~ .x %in% names(summary_info)
  )
  purrr::walk2(
    expected_summary_fields,
    summary_checks,
    ~ {
      testthat::expect_true(.y, info = paste("Should have", .x, "in summary"))
    }
  )
  
})

# NEW TESTS FOR METADATA EXTRACTION

testthat::test_that("extract_pro_metadata handles different tow line formats", {
  # Create temporary PRO files with different tow line formats
  
  # Format 1: %	Tow:	1  AE  AE2214
  temp_file1 <- tempfile(fileext = ".pro")
  writeLines(c(
    "%	Tow:	1  AE  AE2214",
    "%	Date:	7/15/2022",
    "%	Temperature	Probe	#		6116",
    "time	pres	temp	sal",
    "1.0	0.5	15.2	35.1"
  ), temp_file1)
  
  metadata1 <- ecotaxaLoadR:::extract_pro_metadata(temp_file1)
  
  testthat::expect_equal(metadata1$tow_number, 1)
  testthat::expect_equal(metadata1$vessel, "AE")
  testthat::expect_equal(metadata1$cruise_id, "AE2214")
  
  # Format 2: %	Tow:	13  Sally Ride  SR2408
  temp_file2 <- tempfile(fileext = ".pro")
  writeLines(c(
    "%	Tow:	13  Sally Ride  SR2408",
    "%	Date:	5/18/2024",
    "%	Temperature	Probe	#		4060",
    "time	pres	temp	sal",
    "1.0	0.5	15.2	35.1"
  ), temp_file2)
  
  metadata2 <- ecotaxaLoadR:::extract_pro_metadata(temp_file2)
  
  testthat::expect_equal(metadata2$tow_number, 13)
  testthat::expect_equal(metadata2$vessel, "Sally Ride")
  testthat::expect_equal(metadata2$cruise_id, "SR2408")
  
  # Clean up
  unlink(temp_file1)
  unlink(temp_file2)
})

testthat::test_that("extract_pro_metadata handles edge cases in tow line", {
  # Test with multi-word vessel name
  temp_file1 <- tempfile(fileext = ".pro")
  writeLines(c(
    "%	Tow:	5  R/V Atlantis  AT2023",
    "%	Date:	3/10/2023",
    "time	pres	temp	sal",
    "1.0	0.5	15.2	35.1"
  ), temp_file1)
  
  metadata1 <- ecotaxaLoadR:::extract_pro_metadata(temp_file1)
  
  testthat::expect_equal(metadata1$tow_number, 5)
  testthat::expect_equal(metadata1$vessel, "R/V Atlantis")
  testthat::expect_equal(metadata1$cruise_id, "AT2023")
  
  # Test with only vessel (no cruise)
  temp_file2 <- tempfile(fileext = ".pro")
  writeLines(c(
    "%	Tow:	2  ShipName",
    "%	Date:	1/1/2024",
    "time	pres	temp	sal",
    "1.0	0.5	15.2	35.1"
  ), temp_file2)
  
  metadata2 <- ecotaxaLoadR:::extract_pro_metadata(temp_file2)
  
  testthat::expect_equal(metadata2$tow_number, 2)
  testthat::expect_equal(metadata2$vessel, "ShipName")
  testthat::expect_true(is.na(metadata2$cruise_id))
  
  # Clean up
  unlink(temp_file1)
  unlink(temp_file2)
})

testthat::test_that("extract_pro_metadata extracts all metadata fields correctly", {
  # Create a comprehensive test file
  temp_file <- tempfile(fileext = ".pro")
  writeLines(c(
    "%	Tow:	7  Test Vessel  TC2024",
    "%	Date:	6/15/2024",
    "%	Temperature	Probe	#		1234				Conductivity	Probe	#		5678",
    "%	Pressure	Probe	#		9012				Oxygen	Probe		#		3456",
    "%	Transmissometer		#						Fluorometer	#		SCF1234				Irradiance	Probe	#",
    "%	Flow	Meter	Calibration		3.45	(m/count)",
    "time	pres	temp	sal",
    "1.0	0.5	15.2	35.1"
  ), temp_file)
  
  metadata <- ecotaxaLoadR:::extract_pro_metadata(temp_file)
  
  # Test all fields
  testthat::expect_equal(metadata$tow_number, 7)
  testthat::expect_equal(metadata$vessel, "Test Vessel")
  testthat::expect_equal(metadata$cruise_id, "TC2024")
  testthat::expect_equal(metadata$temperature_probe, "1234")
  testthat::expect_equal(metadata$conductivity_probe, "5678")
  testthat::expect_equal(metadata$pressure_probe, "9012")
  testthat::expect_equal(metadata$oxygen_probe, "3456")
  testthat::expect_equal(metadata$fluorometer, "SCF1234")
  testthat::expect_equal(metadata$flow_meter_calibration, 3.45)
  testthat::expect_equal(metadata$flow_meter_units, "(m/count)")
  
  # Test date parsing
  testthat::expect_s3_class(metadata$date, "Date")
  testthat::expect_equal(as.character(metadata$date), "2024-06-15")
  
  # Clean up
  unlink(temp_file)
})

testthat::test_that("extract_pro_metadata handles missing fields gracefully", {
  # Create a minimal test file
  temp_file <- tempfile(fileext = ".pro")
  writeLines(c(
    "%	Tow:	1  Ship  Cruise",
    "time	pres	temp	sal",
    "1.0	0.5	15.2	35.1"
  ), temp_file)
  
  metadata <- ecotaxaLoadR:::extract_pro_metadata(temp_file)
  
  # Should have tow info but missing other fields should be NULL or NA
  testthat::expect_equal(metadata$tow_number, 1)
  testthat::expect_equal(metadata$vessel, "Ship")
  testthat::expect_equal(metadata$cruise_id, "Cruise")
  
  # Missing fields should be NULL (not present in list)
  testthat::expect_null(metadata$date)
  testthat::expect_null(metadata$temperature_probe)
  testthat::expect_null(metadata$flow_meter_calibration)
  
  # Clean up
  unlink(temp_file)
})

testthat::test_that("Integration: PRO files with new tow formats work end-to-end", {
  # Create a test PRO file with new format
  temp_file <- tempfile(fileext = ".pro")
  writeLines(c(
    "%	Tow:	3  AE  AE2214",
    "%	Date:	8/1/2024",
    "%	Temperature	Probe	#		1111				Conductivity	Probe	#		2222",
    "%	Flow	Meter	Calibration		2.75	(m/count)",
    "time	pres	temp	sal	lat	lon",
    "1.0	0.5	15.2	35.1	34.0	-118.0",
    "1.1	1.0	15.1	35.2	34.0	-118.0"
  ), temp_file)
  
  # Test full ingestion
  result <- ecotaxaLoadR::ingest_pro_file(temp_file)
  
  # Check that result is valid
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_gt(nrow(result), 0)
  
  # Check that metadata was extracted correctly
  testthat::expect_equal(unique(result$tow_number), 3)
  testthat::expect_equal(unique(result$vessel), "AE")
  testthat::expect_equal(unique(result$cruise_id), "AE2214")
  testthat::expect_equal(unique(result$flow_meter_calibration), 2.75)
  
  # Check required columns exist
  required_cols <- c(
    "datetime_gmt",
    "datetime_local",
    "timezone",
    "file_name"
  )
  purrr::walk(required_cols, ~ {
    testthat::expect_true(.x %in% names(result), 
                         info = paste("Should have", .x, "column"))
  })
  
  # Clean up
  unlink(temp_file)
})

testthat::test_that("load_pro_files and ingest_pro_file pass daynight parameter correctly", {
  # Create a test directory with a PRO file
  temp_dir <- file.path(tempdir(), "test_pro_daynight")
  dir.create(temp_dir, showWarnings = FALSE)
  
  temp_file <- file.path(temp_dir, "test.pro")
  writeLines(c(
    "%	Tow:	1  TestShip  TC001",
    "%	Date:	6/15/2024",
    "time	pres	temp	sal	lat	lon",
    "1.0	0.5	15.2	35.1	34.0	-118.0",
    "1.1	1.0	15.1	35.2	34.0	-118.0"
  ), temp_file)
  
  # Test that load_pro_files passes daynight parameter to ingest_pro_file
  result_daynight_true <- ecotaxaLoadR::load_pro_files(temp_dir, daynight = TRUE)
  result_daynight_false <- ecotaxaLoadR::load_pro_files(temp_dir, daynight = FALSE)
  
  # Both should succeed
  testthat::expect_s3_class(result_daynight_true, "data.frame")
  testthat::expect_s3_class(result_daynight_false, "data.frame")
  testthat::expect_equal(nrow(result_daynight_true), nrow(result_daynight_false))
  
  # Clean up
  unlink(temp_dir, recursive = TRUE)
})

testthat::test_that("ingest_pro_file extracts MOC number from different filename formats", {
  # Test traditional MOC format
  temp_file1 <- tempfile(fileext = ".PRO", pattern = "MOC8_08A")
  writeLines(c(
    "%	Tow:	1  TestShip  TC001",
    "%	Date:	6/15/2024",
    "time	pres	temp	sal	lat	lon",
    "1.0	0.5	15.2	35.1	34.0	-118.0"
  ), temp_file1)
  
  result1 <- ecotaxaLoadR::ingest_pro_file(temp_file1)
  testthat::expect_equal(unique(result1$moc), 8)

  # Test new M format
  temp_file2 <- tempfile(fileext = ".PRO", pattern = "M35_01A")
  writeLines(c(
    "%	Tow:	1  TestShip  TC001",
    "%	Date:	6/15/2024",
    "time	pres	temp	sal	lat	lon",
    "1.0	0.5	15.2	35.1	34.0	-118.0"
  ), temp_file2)
  
  result2 <- ecotaxaLoadR::ingest_pro_file(temp_file2)
  testthat::expect_equal(unique(result2$moc), 35)
  
  # Test case-insensitive
  temp_file3 <- tempfile(fileext = ".PRO", pattern = "m12_03B")
  writeLines(c(
    "%	Tow:	1  TestShip  TC001",
    "%	Date:	6/15/2024",
    "time	pres	temp	sal	lat	lon",
    "1.0	0.5	15.2	35.1	34.0	-118.0"
  ), temp_file3)
  
  result3 <- ecotaxaLoadR::ingest_pro_file(temp_file3)
  testthat::expect_equal(unique(result3$moc), 12)
  
  # Clean up
  unlink(c(temp_file1, temp_file2, temp_file3))
})

testthat::test_that("ingest_pro_file handles filenames without MOC number", {
  # Test filename with no recognizable MOC pattern
  temp_file <- tempfile(fileext = ".PRO", pattern = "data_file")
  writeLines(c(
    "%	Tow:	1  TestShip  TC001",
    "%	Date:	6/15/2024",
    "time	pres	temp	sal	lat	lon",
    "1.0	0.5	15.2	35.1	34.0	-118.0"
  ), temp_file)
  
  result <- ecotaxaLoadR::ingest_pro_file(temp_file)
  testthat::expect_true(is.na(unique(result$moc)))
  
  # Clean up
  unlink(temp_file)
})