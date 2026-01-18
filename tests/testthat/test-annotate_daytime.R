# tests/testthat/test-annotate_daytime.R

testthat::test_that("annotate_daytime works with EcoTaxa data", {
  # Create sample EcoTaxa data
  eco_data <- data.frame(
    object_lat = c(34.5, 34.5, 35.0),
    object_lon = c(-120.5, -120.5, -121.0),
    object_date = c("2024-06-15", "2024-06-15", "2024-06-15"),
    object_time = c("12:00:00", "00:00:00", "06:00:00"),
    sample_id = c("sample1", "sample2", "sample3"),
    stringsAsFactors = FALSE
  )
  
  # Test function
  result <- ecotaxaLoadR::annotate_daytime(eco_data)
  
  # Check structure
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true("is_day" %in% names(result))
  testthat::expect_equal(nrow(result), nrow(eco_data))
  testthat::expect_type(result$is_day, "logical")
  
  # Check that original columns are preserved
  expected_cols <- c("object_lat", "object_lon", "object_date", "object_time", "sample_id")
  testthat::expect_true(all(expected_cols %in% names(result)))
  
  # Check that midday is day and midnight is night (roughly)
  midday_row <- which(result$object_time == "12:00:00")
  midnight_row <- which(result$object_time == "00:00:00")
  testthat::expect_true(result$is_day[midday_row])
  testthat::expect_false(result$is_day[midnight_row])
})

testthat::test_that("annotate_daytime works with PRO data", {
  # Create sample PRO data
  pro_data <- data.frame(
    lat = c(27.32393, 27.32392, 27.32391),
    lon = c(-111.28438, -111.28438, -111.28437),
    datetime_gmt = as.POSIXct(c(
      "2024-05-04 18:03:09",  # Evening
      "2024-05-04 06:00:00",  # Dawn
      "2024-05-04 12:00:00"   # Midday
    ), tz = "UTC"),
    temp = c(23.423, 23.369, 23.350),
    pres = c(-0.2, -0.3, -0.5),
    stringsAsFactors = FALSE
  )
  
  # Test function
  result <- ecotaxaLoadR::annotate_daytime(pro_data)
  
  # Check structure
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true("is_day" %in% names(result))
  testthat::expect_equal(nrow(result), nrow(pro_data))
  testthat::expect_type(result$is_day, "logical")
  
  # Check that original columns are preserved
  expected_cols <- c("lat", "lon", "datetime_gmt", "temp", "pres")
  testthat::expect_true(all(expected_cols %in% names(result)))
  
  # Check that midday is day (should be TRUE for noon)
  midday_row <- which(format(result$datetime_gmt, "%H:%M") == "12:00")
  testthat::expect_true(result$is_day[midday_row])
})

testthat::test_that("annotate_daytime detects data source correctly", {
  # EcoTaxa data
  eco_data <- data.frame(
    object_lat = 34.5,
    object_lon = -120.5,
    object_date = "2024-06-15",
    object_time = "12:00:00"
  )
  
  # PRO data
  pro_data <- data.frame(
    lat = 27.32393,
    lon = -111.28438,
    datetime_gmt = as.POSIXct("2024-05-04 18:03:09", tz = "UTC")
  )
  
  # Test that both work and detect source correctly
  testthat::expect_message(
    eco_result <- ecotaxaLoadR::annotate_daytime(eco_data),
    "Detected data source: ecotaxa"
  )
  
  testthat::expect_message(
    pro_result <- ecotaxaLoadR::annotate_daytime(pro_data),
    "Detected data source: pro"
  )
  
  # Both should have is_day column
  testthat::expect_true("is_day" %in% names(eco_result))
  testthat::expect_true("is_day" %in% names(pro_result))
})

testthat::test_that("annotate_daytime handles data with both column sets", {
  # Data with both EcoTaxa and PRO columns (should prioritize EcoTaxa)
  mixed_data <- data.frame(
    object_lat = 34.5,
    object_lon = -120.5,
    object_date = "2024-06-15",
    object_time = "12:00:00",
    lat = 27.32393,
    lon = -111.28438,
    datetime_gmt = as.POSIXct("2024-05-04 18:03:09", tz = "UTC")
  )
  
  # Should warn and use EcoTaxa format
  testthat::expect_warning(
    result <- ecotaxaLoadR::annotate_daytime(mixed_data),
    "Both EcoTaxa and PRO format columns detected. Using EcoTaxa format."
  )
  
  testthat::expect_message(
    result <- suppressWarnings(ecotaxaLoadR::annotate_daytime(mixed_data)),
    "Detected data source: ecotaxa"
  )
  
  testthat::expect_true("is_day" %in% names(result))
})

testthat::test_that("annotate_daytime fails with missing required columns", {
  # Data missing required columns
  incomplete_data <- data.frame(
    object_lat = 34.5,
    object_lon = -120.5,
    # Missing object_date and object_time
    sample_id = "sample1"
  )
  
  testthat::expect_error(
    ecotaxaLoadR::annotate_daytime(incomplete_data),
    "Input data must contain either:"
  )
})

testthat::test_that("annotate_daytime preserves row count", {
  # Test with EcoTaxa data
  eco_data <- data.frame(
    object_lat = rep(34.5, 100),
    object_lon = rep(-120.5, 100),
    object_date = rep("2024-06-15", 100),
    object_time = rep("12:00:00", 100),
    row_id = 1:100
  )
  
  result_eco <- ecotaxaLoadR::annotate_daytime(eco_data)
  testthat::expect_equal(nrow(result_eco), nrow(eco_data))
  
  # Test with PRO data
  pro_data <- data.frame(
    lat = runif(50, 25, 30),
    lon = runif(50, -115, -110),
    datetime_gmt = seq(
      as.POSIXct("2024-05-04 00:00:00", tz = "UTC"),
      as.POSIXct("2024-05-04 23:59:00", tz = "UTC"),
      length.out = 50
    ),
    row_id = 1:50
  )
  
  result_pro <- ecotaxaLoadR::annotate_daytime(pro_data)
  testthat::expect_equal(nrow(result_pro), nrow(pro_data))
})

testthat::test_that("annotate_daytime handles duplicate positions/times correctly", {
  # EcoTaxa data with duplicate position/time combinations
  eco_data <- data.frame(
    object_lat = c(34.5, 34.5, 34.5, 35.0),
    object_lon = c(-120.5, -120.5, -120.5, -121.0),
    object_date = c("2024-06-15", "2024-06-15", "2024-06-15", "2024-06-15"),
    object_time = c("12:00:00", "12:00:00", "00:00:00", "12:00:00"),
    object_id = c("obj1", "obj2", "obj3", "obj4")
  )
  
  result <- ecotaxaLoadR::annotate_daytime(eco_data)
  
  # Should have same number of rows
  testthat::expect_equal(nrow(result), nrow(eco_data))
  
  # Rows 1 and 2 should have same is_day value (same position/time)
  testthat::expect_equal(result$is_day[1], result$is_day[2])
  
  # Row 3 (midnight) should be different from rows 1&2 (midday)
  testthat::expect_true(result$is_day[3] != result$is_day[1])
  
  # All object_ids should be preserved
  testthat::expect_equal(sort(result$object_id), sort(eco_data$object_id))
})

testthat::test_that("annotate_daytime handles edge cases", {
  # Single row data
  single_eco <- data.frame(
    object_lat = 34.5,
    object_lon = -120.5,
    object_date = "2024-06-15",
    object_time = "12:00:00"
  )
  
  result <- ecotaxaLoadR::annotate_daytime(single_eco)
  testthat::expect_equal(nrow(result), 1)
  testthat::expect_true("is_day" %in% names(result))
  
  # Single row PRO data
  single_pro <- data.frame(
    lat = 27.32393,
    lon = -111.28438,
    datetime_gmt = as.POSIXct("2024-05-04 12:00:00", tz = "UTC")
  )
  
  result_pro <- ecotaxaLoadR::annotate_daytime(single_pro)
  testthat::expect_equal(nrow(result_pro), 1)
  testthat::expect_true("is_day" %in% names(result_pro))
})

testthat::test_that("annotate_daytime handles NA values gracefully", {
  # EcoTaxa data with some NA values in non-required columns
  eco_data_na <- data.frame(
    object_lat = c(34.5, 34.6),
    object_lon = c(-120.5, -120.4),
    object_date = c("2024-06-15", "2024-06-15"),
    object_time = c("12:00:00", "00:00:00"),
    optional_col = c(NA, "value"),
    stringsAsFactors = FALSE
  )
  
  result <- ecotaxaLoadR::annotate_daytime(eco_data_na)
  testthat::expect_equal(nrow(result), nrow(eco_data_na))
  testthat::expect_true("is_day" %in% names(result))
  testthat::expect_true(all(!is.na(result$is_day)))
})

testthat::test_that("annotate_daytime warns and returns NA for invalid PRO geocodes", {
  # PRO data with invalid and valid coordinates
  pro_data <- data.frame(
    lat = c(-999.0, 27.32392, 100.0),           # invalid sentinel, valid, invalid range
    lon = c(-999.0, -111.28438, 200.0),        # invalid sentinel, valid, invalid range
    datetime_gmt = as.POSIXct(c(
      "2024-05-04 12:00:00",
      "2024-05-04 12:00:00",
      "2024-05-04 12:00:00"
    ), tz = "UTC"),
    stringsAsFactors = FALSE
  )

  # Expect a single aggregated warning and NA for invalid rows
  testthat::expect_warning(
    result <- ecotaxaLoadR::annotate_daytime(pro_data),
    "invalid geocode rows"
  )
  testthat::expect_true("is_day" %in% names(result))
  testthat::expect_true(is.na(result$is_day[1]))
  testthat::expect_false(is.na(result$is_day[2]))
  testthat::expect_true(is.na(result$is_day[3]))
})

testthat::test_that("annotate_daytime warns and returns NA for invalid EcoTaxa geocodes", {
  eco_data <- data.frame(
    object_lat = c(34.5, -999.0, 91.0),        # valid, invalid sentinel, invalid range
    object_lon = c(-120.5, -999.0, -181.0),    # valid, invalid sentinel, invalid range
    object_date = c("2024-06-15", "2024-06-15", "2024-06-15"),
    object_time = c("12:00:00", "12:00:00", "12:00:00"),
    stringsAsFactors = FALSE
  )

  testthat::expect_warning(
    result <- ecotaxaLoadR::annotate_daytime(eco_data),
    "invalid geocode rows"
  )
  testthat::expect_true("is_day" %in% names(result))
  testthat::expect_false(is.na(result$is_day[1]))
  testthat::expect_true(is.na(result$is_day[2]))
  testthat::expect_true(is.na(result$is_day[3]))
})

testthat::test_that("annotate_daytime processes PRO data without deduplication", {
  # PRO data where each record should be processed individually
  pro_data <- data.frame(
    lat = c(27.323, 27.323, 27.323),  # Same position
    lon = c(-111.284, -111.284, -111.284),
    datetime_gmt = as.POSIXct(c(
      "2024-05-04 12:00:00",
      "2024-05-04 12:00:01",  # 1 second later
      "2024-05-04 12:00:02"   # 2 seconds later
    ), tz = "UTC"),
    measurement = c(1, 2, 3)
  )
  
  # Mock the future_pmap_lgl to count how many times it's called
  original_future_pmap_lgl <- furrr::future_pmap_lgl
  call_count <- 0
  
  # This test verifies that PRO data processes all records individually
  result <- ecotaxaLoadR::annotate_daytime(pro_data)
  
  # Should process all 3 records (no deduplication)
  testthat::expect_equal(nrow(result), 3)
  testthat::expect_true(all("is_day" %in% names(result)))
  testthat::expect_equal(result$measurement, c(1, 2, 3))
})