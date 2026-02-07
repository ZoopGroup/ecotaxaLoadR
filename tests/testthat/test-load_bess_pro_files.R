# Tests for BESS PRO file loading functions

library(testthat)
library(dplyr)
library(stringr)

# Test extract_bess_pro_metadata function
test_that("extract_bess_pro_metadata correctly parses BESS format headers", {
  
  # Test with M-01-005.PRO
  test_file <- here::here("tests", "testthat", "test-data", "bess_pro_files", "M-01-005.PRO")
  skip_if_not(file.exists(test_file), "Test file M-01-005.PRO not found")
  
  metadata <- extract_bess_pro_metadata(test_file)
  
  # Check basic structure
  expect_type(metadata, "list")
  expect_equal(metadata$file_name, "M-01-005.PRO")
  expect_equal(metadata$file_path, test_file)
  
  # Check tow and vessel extraction
  expect_equal(metadata$tow_id, "M-01-005")
  expect_equal(metadata$vessel, "Oceanus")
  
  # Check date parsing
  expect_equal(metadata$date_string, "13 Aug 2011")
  expect_false(is.na(metadata$parsed_date))
  expect_equal(as.Date(metadata$parsed_date, origin = "1970-01-01"), 
               as.Date("2011-08-13"))
  
  # Check instrument probes
  expect_type(metadata$instrument_probes, "list")
  expect_gt(length(metadata$instrument_probes), 0)
  
  # Verify some expected instruments
  instrument_names <- map_chr(metadata$instrument_probes, "instrument")
  expect_true("Temperature" %in% instrument_names)
  expect_true("Conductivity" %in% instrument_names)
  expect_true("Pressure" %in% instrument_names)
  
  # Check column headers
  expect_true(length(metadata$column_headers) > 0)
  expect_true("time" %in% metadata$column_headers)
  expect_true("lat" %in% metadata$column_headers)
  expect_true("lon" %in% metadata$column_headers)
  expect_equal(metadata$num_columns, length(metadata$column_headers))
})

test_that("extract_bess_pro_metadata works with M_01_007.PRO", {
  
  test_file <- here::here("tests", "testthat", "test-data", "bess_pro_files", "M_01_007.PRO")
  skip_if_not(file.exists(test_file), "Test file M_01_007.PRO not found")
  
  metadata <- extract_bess_pro_metadata(test_file)
  
  # Check tow and vessel extraction for different format
  expect_equal(metadata$tow_id, "M-01-007")
  expect_equal(metadata$vessel, "New Horizon 1208")
  
  # Check date parsing for September date
  expect_equal(metadata$date_string, "2 Sept 2012")
  expect_false(is.na(metadata$parsed_date))
  expect_equal(as.Date(metadata$parsed_date, origin = "1970-01-01"), 
               as.Date("2012-09-02"))
  
  # Check flow meter calibration parsing
  expect_false(is.na(metadata$flow_meter_calibration))
  expect_equal(metadata$flow_meter_calibration, 6.3882)
})

test_that("extract_bess_pro_metadata handles missing or malformed data gracefully", {
  
  # Create temporary test file with minimal content
  temp_file <- tempfile(fileext = ".PRO")
  writeLines(c("% Tow: TEST-001   TestVessel",
               "% Date: Invalid Date Format",
               "% time    pres    temp"), temp_file)
  
  metadata <- extract_bess_pro_metadata(temp_file)
  
  expect_equal(metadata$tow_id, "TEST-001")
  expect_equal(metadata$vessel, "TestVessel")
  expect_equal(metadata$date_string, "Invalid Date Format")
  expect_true(is.na(metadata$parsed_date))
  
  # Clean up
  unlink(temp_file)
})

# Test ingest_bess_pro_file function
test_that("ingest_bess_pro_file successfully reads and processes BESS files", {
  
  test_file <- here::here("tests", "testthat", "test-data", "bess_pro_files", "M-01-005.PRO")
  skip_if_not(file.exists(test_file), "Test file M-01-005.PRO not found")
  
  pro_data <- ingest_bess_pro_file(test_file)
  
  # Check data structure
  expect_s3_class(pro_data, "data.frame")
  expect_gt(nrow(pro_data), 0)
  
  # Check metadata columns are added
  expect_true("file_name" %in% names(pro_data))
  expect_true("tow_id" %in% names(pro_data))
  expect_true("vessel" %in% names(pro_data))
  expect_true("date_string" %in% names(pro_data))
  expect_true("parsed_date" %in% names(pro_data))
  
  # Check data columns are present
  expect_true("time" %in% names(pro_data))
  expect_true("lat" %in% names(pro_data))
  expect_true("lon" %in% names(pro_data))
  expect_true("pres" %in% names(pro_data))
  expect_true("temp" %in% names(pro_data))
  
  # Check datetime conversion
  expect_true("decimal_time" %in% names(pro_data))
  expect_true("datetime_gmt" %in% names(pro_data))
  
  # Verify metadata values
  expect_equal(unique(pro_data$tow_id), "M-01-005")
  expect_equal(unique(pro_data$vessel), "Oceanus")
  
  # Check data types
  expect_type(pro_data$time, "double")
  expect_type(pro_data$lat, "double") 
  expect_type(pro_data$lon, "double")
  expect_s3_class(pro_data$datetime_gmt, "POSIXct")
  
  # Check coordinate ranges are reasonable
  expect_true(all(pro_data$lat > 0 & pro_data$lat < 90, na.rm = TRUE))
  expect_true(all(pro_data$lon < 0, na.rm = TRUE)) # Pacific/Atlantic
})

test_that("ingest_bess_pro_file handles both test files correctly", {
  
  # Test M_01_007.PRO
  test_file2 <- here::here("tests", "testthat", "test-data", "bess_pro_files", "M_01_007.PRO")
  skip_if_not(file.exists(test_file2), "Test file M_01_007.PRO not found")
  
  pro_data2 <- ingest_bess_pro_file(test_file2)
  
  expect_s3_class(pro_data2, "data.frame")
  expect_gt(nrow(pro_data2), 0)
  expect_equal(unique(pro_data2$tow_id), "M-01-007")
  expect_equal(unique(pro_data2$vessel), "New Horizon 1208")
  
  # Check that both files have similar structure but different data
  test_file1 <- here::here("tests", "testthat", "test-data", "bess_pro_files", "M-01-005.PRO")
  if (file.exists(test_file1)) {
    pro_data1 <- ingest_bess_pro_file(test_file1)
    
    # Same columns
    expect_equal(sort(names(pro_data1)), sort(names(pro_data2)))
    
    # Different vessels and dates
    expect_false(unique(pro_data1$vessel) == unique(pro_data2$vessel))
    expect_false(unique(pro_data1$parsed_date) == unique(pro_data2$parsed_date))
  }
})

# Test load_bess_pro_files function
test_that("load_bess_pro_files processes multiple files correctly", {
  
  test_dir <- here::here("tests", "testthat", "test-data", "bess_pro_files")
  skip_if_not(dir.exists(test_dir), "Test directory not found")
  
  # Get BESS format files (exclude SIO format files)
  bess_files <- list.files(test_dir, pattern = "^M[-_].*\\.PRO$", full.names = TRUE)
  skip_if(length(bess_files) == 0, "No BESS test files found")
  
  # Test with file list
  combined_data <- load_bess_pro_files(bess_files, 
                                       daynight = FALSE,
                                       progress = FALSE)
  
  expect_s3_class(combined_data, "data.frame")
  expect_gt(nrow(combined_data), 0)
  expect_equal(length(unique(combined_data$file_name)), length(bess_files))
  
  # Check all expected columns are present
  expected_cols <- c(
    "file_name", "tow_id", "vessel", "date_string", "parsed_date",
    "time", "decimal_time", "datetime_gmt", "datetime_local", "timezone",
    "lat", "lon", "pres", "temp"
  )
  expect_true(all(expected_cols %in% names(combined_data)))
  
  # Test with directory path
  combined_data2 <- load_bess_pro_files(test_dir, 
                                        pattern = "^M[-_].*\\.PRO$",
                                        daynight = FALSE,
                                        progress = FALSE)
  
  expect_equal(nrow(combined_data), nrow(combined_data2))
  expect_equal(sort(unique(combined_data$file_name)), 
               sort(unique(combined_data2$file_name)))
})

test_that("load_bess_pro_files adds day/night annotation when requested", {
  
  test_dir <- here::here("tests", "testthat", "test-data", "bess_pro_files")
  skip_if_not(dir.exists(test_dir), "Test directory not found")
  
  bess_files <- list.files(test_dir, pattern = "^M[-_].*\\.PRO$", full.names = TRUE)
  skip_if(length(bess_files) == 0, "No BESS test files found")
  
  # Load with day/night annotation
  combined_data <- load_bess_pro_files(bess_files, 
                                       daynight = TRUE,
                                       progress = FALSE)
  
  # Check day/night columns are added
  expect_true("is_day" %in% names(combined_data))
  expect_true(all(combined_data$is_day %in% c(TRUE, FALSE), na.rm = TRUE))
})

test_that("load_bess_pro_files annotates and warns for invalid coords", {
  # Create a temporary minimal BESS-style file with invalid and valid coords
  temp_file <- tempfile(fileext = ".PRO")
  writeLines(c(
    "% Tow: TEST-INV   TestVessel",
    "% Date: 1 Jan 2020",
    "% time    pres    temp    lat    lon",
    "1.0 0.0 15.0 -999.0 -999.0",
    "1.1 1.0 14.9 34.0 -120.0"
  ), temp_file)

  # Load with daynight annotation and expect a warning
  expect_warning(
    combined_data <- load_bess_pro_files(c(temp_file), daynight = TRUE, progress = FALSE),
    "invalid geocode rows"
  )
  expect_true("is_day" %in% names(combined_data))
  # First row invalid -> NA, second row valid -> not NA
  expect_true(is.na(combined_data$is_day[1]))
  expect_false(is.na(combined_data$is_day[2]))

  unlink(temp_file)
})

test_that("load_bess_pro_files error handling works correctly", {
  
  # Test with non-existent directory
  expect_error(load_bess_pro_files("/non/existent/path"), 
               "Files not found")
  
  # Test with non-existent files
  expect_error(load_bess_pro_files(c("/fake/file1.PRO", "/fake/file2.PRO")), 
               "Files not found")
  
  # Test with empty file list
  expect_error(load_bess_pro_files(character(0)), 
               "No files found to process")
})

# Integration test with annotate_daytime
test_that("BESS workflow integrates correctly with annotate_daytime", {
  
  test_dir <- here::here("tests", "testthat", "test-data", "bess_pro_files")
  skip_if_not(dir.exists(test_dir), "Test directory not found")
  
  bess_files <- list.files(test_dir, pattern = "^M[-_].*\\.PRO$", full.names = TRUE)
  skip_if(length(bess_files) == 0, "No BESS test files found")
  
  # Load without daynight first
  pro_data <- load_bess_pro_files(bess_files, 
                                  daynight = FALSE,
                                  progress = FALSE)
  
  # Apply annotate_daytime separately
  annotated_data <- annotate_daytime(pro_data)
  
  # Compare with built-in annotation
  built_in_data <- load_bess_pro_files(bess_files, 
                                       daynight = TRUE,
                                       progress = FALSE)
  
  # Should have same day/night classifications
  expect_equal(annotated_data$is_day, built_in_data$is_day)
})

# Performance and edge case tests  
test_that("BESS functions handle edge cases appropriately", {
  
  # Test with file that has minimal data
  temp_file <- tempfile(fileext = ".PRO")
  writeLines(c("% Tow: MIN-001   TestVessel", 
               "% Date: 1 Jan 2020",
               "% time pres temp",
               "1.0 0.0 15.0",
               "1.1 1.0 14.9"), temp_file)
  
  pro_data <- ingest_bess_pro_file(temp_file)
  expect_equal(nrow(pro_data), 2)
  expect_equal(unique(pro_data$tow_id), "MIN-001")
  
  # Clean up
  unlink(temp_file)
})

test_that("BESS format detection distinguishes from SIO format", {
  
  # This test ensures BESS files are structurally different from SIO
  test_dir <- here::here("tests", "testthat", "test-data", "bess_pro_files")
  skip_if_not(dir.exists(test_dir), "Test directory not found")
  
  # Get potential SIO file (different naming pattern)
  sio_files <- list.files(test_dir, pattern = "^[A-Z]+[0-9]+.*\\.PRO$", full.names = TRUE)
  bess_files <- list.files(test_dir, pattern = "^M[-_].*\\.PRO$", full.names = TRUE)
  
  if (length(sio_files) > 0 && length(bess_files) > 0) {
    
    # BESS metadata should work on BESS files
    bess_meta <- extract_bess_pro_metadata(bess_files[1])
    expect_false(is.na(bess_meta$tow_id))
    expect_true(str_detect(bess_meta$tow_id, "^M[-_]"))
    
    # BESS metadata should not work well on SIO files (different format)
    sio_meta <- extract_bess_pro_metadata(sio_files[1])
    # SIO files likely won't have the BESS-specific header patterns
    expect_true(is.na(sio_meta$tow_id) || 
                !str_detect(sio_meta$tow_id %||% "", "^M[-_]"))
  }
})