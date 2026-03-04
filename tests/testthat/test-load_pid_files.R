testthat::test_that("load_pid_files works with test directory containing PID files", {
  # Test with directory containing both PID files
  test_dir <- testthat::test_path("test-data", "pid_files")
  
  testthat::expect_true(dir.exists(test_dir))
  
  result <- ecotaxaLoadR::load_pid_files(test_dir)
  
  # Check that result is a list
  testthat::expect_type(result, "list")
  
  # Check for expected components
  expected_components <- c("metadata", "records", "attributes", "file_count", "processed_files")
  components_present <- purrr::map_lgl(expected_components, ~ .x %in% names(result))
  purrr::walk2(expected_components, components_present, ~ {
    testthat::expect_true(.y, info = paste("Should have", .x, "component"))
  })
  
  # Check that we processed some files
  testthat::expect_gt(result$file_count, 0)
  
  # Each data component should be a tibble
  data_components <- c("metadata", "records", "attributes")
  purrr::walk(data_components, ~ {
    testthat::expect_s3_class(result[[.x]], "data.frame")
  })
})

testthat::test_that("load_pid_files handles empty directory", {
  # Create a temporary directory with no PID files
  temp_dir <- tempdir()
  empty_dir <- file.path(temp_dir, "empty_pid_dir")
  dir.create(empty_dir, showWarnings = FALSE)
  
  testthat::expect_warning(
    result <- ecotaxaLoadR::load_pid_files(empty_dir),
    "No PID files found"
  )
  
  # Should return empty tibbles
  testthat::expect_equal(nrow(result$metadata), 0)
  testthat::expect_equal(nrow(result$records), 0)
  testthat::expect_equal(nrow(result$attributes), 0)
  
  # Clean up
  unlink(empty_dir, recursive = TRUE)
})

testthat::test_that("parse_pid_file works with sr2407 PID file", {
  # Test with sr2407 file
  pid_file <- testthat::test_path("test-data", "pid_files", "sr2407_m6_n3_d2_b_1_dat1.pid")
  
  testthat::expect_true(file.exists(pid_file))
  
  result <- ecotaxaLoadR:::parse_pid_file(pid_file)
  
  # Check that result is a list with expected components
  expected_components <- c("image_metadata", "image_records", "image_attributes")
  components_present <- purrr::map_lgl(expected_components, ~ .x %in% names(result))
  testthat::expect_true(all(components_present))
  
  # Each component should be a data frame
  purrr::walk(expected_components, ~ {
    testthat::expect_s3_class(result[[.x]], "data.frame")
  })
  
  # Check that metadata has expected structure
  metadata <- result$image_metadata
  if (nrow(metadata) > 0) {
    expected_metadata_cols <- c("scan_id", "section_name", "key", "value", "sample_id")
    metadata_cols_present <- purrr::map_lgl(expected_metadata_cols, ~ .x %in% names(metadata))
    testthat::expect_true(all(metadata_cols_present))
    
    # Should have filename record
    testthat::expect_true(any(metadata$key == "filename"))
    testthat::expect_true(any(metadata$section_name == "file_info"))
  }
  
  # Check that records has expected structure
  records <- result$image_records
  if (nrow(records) > 0) {
    expected_record_cols <- c("scan_id", "image_number")
    record_cols_present <- purrr::map_lgl(expected_record_cols, ~ .x %in% names(records))
    testthat::expect_true(all(record_cols_present))
    
    # Image numbers should be integers
    testthat::expect_true(is.integer(records$image_number) || is.numeric(records$image_number))
  }
  
  # Check that attributes has expected structure
  attributes <- result$image_attributes
  if (nrow(attributes) > 0) {
    expected_attr_cols <- c("scan_id", "image_number", "attribute_name", "attribute_value")
    attr_cols_present <- purrr::map_lgl(expected_attr_cols, ~ .x %in% names(attributes))
    testthat::expect_true(all(attr_cols_present))
  }
})

testthat::test_that("parse_pid_file works with sr2408 PID file", {
  # Test with sr2408 file
  pid_file <- testthat::test_path("test-data", "pid_files", "sr2408_m13_n7_d2_a_1_dat1.pid")
  
  testthat::expect_true(file.exists(pid_file))
  
  result <- ecotaxaLoadR:::parse_pid_file(pid_file)
  
  # Check basic structure
  expected_components <- c("image_metadata", "image_records", "image_attributes")
  components_present <- purrr::map_lgl(expected_components, ~ .x %in% names(result))
  testthat::expect_true(all(components_present))
  
  # Check that scan_id is correctly extracted
  metadata <- result$image_metadata
  if (nrow(metadata) > 0) {
    scan_ids <- unique(metadata$scan_id)
    testthat::expect_length(scan_ids, 1)
    testthat::expect_true(grepl("sr2408_m13_n7_d2_a_1", scan_ids[1]))
  }
})

testthat::test_that("parse_pid_file handles missing file", {
  non_existent_file <- testthat::test_path("test-data", "pid_files", "nonexistent.pid")
  
  testthat::expect_error(
    ecotaxaLoadR:::parse_pid_file(non_existent_file)
  )
})

testthat::test_that("parse_metadata_sections_long works correctly", {
  # Create sample metadata lines
  metadata_lines <- c(
    "PID",
    "[Image]",
    "Scanning_date= 20250701_1741",
    "Scanning_area= large",
    "[Sample]", 
    "Project= hypoxia",
    "SampleId= sr2407_m6_n3",
    "Date= 20240504-2248"
  )
  
  result <- ecotaxaLoadR:::parse_metadata_sections_long(
    metadata_lines, 
    "test_scan", 
    "test_file.pid"
  )
  
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  
  # Check expected columns
  expected_cols <- c("scan_id", "section_name", "key", "value", "sample_id")
  cols_present <- purrr::map_lgl(expected_cols, ~ .x %in% names(result))
  testthat::expect_true(all(cols_present))

  # sample_id should be propagated to all rows
  testthat::expect_true(all(result$sample_id == "sr2407_m6_n3"))
  testthat::expect_true(any(result$section_name == "Sample" & result$key == "SampleId"))
  
  # Should have filename record
  testthat::expect_true(any(result$key == "filename"))
  testthat::expect_true(any(result$section_name == "file_info"))
  
  # Should have parsed metadata
  testthat::expect_true(any(result$section_name == "Image"))
  testthat::expect_true(any(result$section_name == "Sample"))
  testthat::expect_true(any(result$key == "Scanning_date"))
  testthat::expect_true(any(result$key == "Project"))
  
  # NEW: Check for parsed sample fields
  testthat::expect_true(any(result$section_name == "parsed_sample"))
  
  parsed_fields <- result[result$section_name == "parsed_sample", ]
  expected_keys <- c("cruise", "moc", "net", "sample_date")
  
  for (key in expected_keys) {
    testthat::expect_true(key %in% parsed_fields$key, 
                         info = paste("Missing parsed field:", key))
  }
  
  # Check specific values
  testthat::expect_equal(parsed_fields$value[parsed_fields$key == "cruise"], "sr2407")
  testthat::expect_equal(parsed_fields$value[parsed_fields$key == "moc"], "6")
  testthat::expect_equal(parsed_fields$value[parsed_fields$key == "net"], "3")
  testthat::expect_equal(parsed_fields$value[parsed_fields$key == "sample_date"], "2024-05-04")
})

testthat::test_that("parse_data_section_long works correctly", {
  # Create sample data
  headers <- c("Item", "Label", "Area", "Mean", "X", "Y")
  data_lines <- c(
    "1;test_001;1234.5;128.5;100;200",
    "2;test_002;2345.6;135.2;150;250"
  )
  
  result <- ecotaxaLoadR:::parse_data_section_long(
    data_lines, 
    headers, 
    "test_scan", 
    "test_file.pid"
  )
  
  testthat::expect_type(result, "list")
  testthat::expect_true("image_records" %in% names(result))
  testthat::expect_true("image_attributes" %in% names(result))
  
  # Check image records
  records <- result$image_records
  testthat::expect_s3_class(records, "data.frame")
  testthat::expect_equal(nrow(records), 2)
  
  expected_record_cols <- c("scan_id", "image_number")
  record_cols_present <- purrr::map_lgl(expected_record_cols, ~ .x %in% names(records))
  testthat::expect_true(all(record_cols_present))
  
  # Check image attributes
  attributes <- result$image_attributes
  testthat::expect_s3_class(attributes, "data.frame")
  testthat::expect_true(nrow(attributes) > 0)
  
  expected_attr_cols <- c("scan_id", "image_number", "attribute_name", "attribute_value")
  attr_cols_present <- purrr::map_lgl(expected_attr_cols, ~ .x %in% names(attributes))
  testthat::expect_true(all(attr_cols_present))
  
  # Should have attributes for both images
  unique_images <- unique(attributes$image_number)
  testthat::expect_length(unique_images, 2)
  testthat::expect_true(all(c(1, 2) %in% unique_images))
})

testthat::test_that("parse_data_section_long handles empty data", {
  result <- ecotaxaLoadR:::parse_data_section_long(
    character(0), 
    character(0), 
    "test_scan", 
    "test_file.pid"
  )
  
  testthat::expect_type(result, "list")
  testthat::expect_equal(nrow(result$image_records), 0)
  testthat::expect_equal(nrow(result$image_attributes), 0)
})

testthat::test_that("parse_data_section_long handles malformed data", {
  headers <- c("Item", "Label", "Area")
  # Line with wrong number of fields
  data_lines <- c(
    "1;test_001;1234.5;extra_field",  # Too many fields
    "2;test_002"  # Too few fields
  )
  
  result <- ecotaxaLoadR:::parse_data_section_long(
    data_lines, 
    headers, 
    "test_scan", 
    "test_file.pid"
  )
  
  # Should handle malformed data gracefully
  testthat::expect_type(result, "list")
  testthat::expect_s3_class(result$image_records, "data.frame")
  testthat::expect_s3_class(result$image_attributes, "data.frame")
})

testthat::test_that("load_pid_files processes both test files successfully", {
  test_dir <- testthat::test_path("test-data", "pid_files")
  
  result <- ecotaxaLoadR::load_pid_files(test_dir)
  
  # Should process both files
  testthat::expect_equal(result$file_count, 2)
  testthat::expect_length(result$processed_files, 2)
  
  # Check that filenames are preserved
  file_names <- result$processed_files
  expected_files <- c("sr2407_m6_n3_d2_b_1_dat1.pid", "sr2408_m13_n7_d2_a_1_dat1.pid")
  files_present <- purrr::map_lgl(expected_files, ~ .x %in% file_names)
  testthat::expect_true(all(files_present))
  
  # Check combined data has records from both files
  metadata <- result$metadata
  if (nrow(metadata) > 0) {
    scan_ids <- unique(metadata$scan_id)
    testthat::expect_true(length(scan_ids) >= 2)
  }
})

testthat::test_that("load_pid_files handles different file patterns", {
  test_dir <- testthat::test_path("test-data", "pid_files")
  
  # Test with specific pattern
  result <- ecotaxaLoadR::load_pid_files(test_dir, file_pattern = "sr2407.*\\.pid$")
  
  # Should only process sr2407 file
  testthat::expect_equal(result$file_count, 1)
  testthat::expect_true(grepl("sr2407", result$processed_files[1]))
})

testthat::test_that("PID parsing preserves data integrity", {
  pid_file <- testthat::test_path("test-data", "pid_files", "sr2407_m6_n3_d2_b_1_dat1.pid")
  
  result <- ecotaxaLoadR:::parse_pid_file(pid_file)
  
  # Check that scan_id is consistent across all components
  scan_ids <- purrr::map(result, ~ if(nrow(.x) > 0) unique(.x$scan_id) else character(0))
  non_empty_scan_ids <- purrr::keep(scan_ids, ~ length(.x) > 0)
  
  if (length(non_empty_scan_ids) > 0) {
    all_scan_ids <- purrr::reduce(non_empty_scan_ids, c)
    testthat::expect_length(unique(all_scan_ids), 1)
  }
  
  # Check that image numbers are consistent between records and attributes
  records <- result$image_records
  attributes <- result$image_attributes
  
  if (nrow(records) > 0 && nrow(attributes) > 0) {
    record_images <- unique(records$image_number)
    attr_images <- unique(attributes$image_number)
    
    # All images in attributes should exist in records
    images_match <- purrr::every(attr_images, ~ .x %in% record_images)
    testthat::expect_true(images_match)
  }
})

testthat::test_that("PID metadata extraction handles various section types", {
  # Test metadata parsing with various section types
  metadata_lines <- c(
    "PID",
    "[Image]",
    "Scanning_date= 20250701_1741",
    "Vuescan_version= 9.7.67",
    "[VueScan]",
    "[Input]", 
    "Source=PerfectionV800",
    "Options=2",
    "[Sample]",
    "Project= hypoxia",
    "SampleId= sr2407_m6_n3"
  )
  
  result <- ecotaxaLoadR:::parse_metadata_sections_long(
    metadata_lines, 
    "test_scan", 
    "test_file.pid"
  )
  
  # Should have multiple sections
  unique_sections <- unique(result$section_name)
  expected_sections <- c("file_info", "Image", "Input", "Sample")
  sections_found <- purrr::map_lgl(expected_sections, ~ .x %in% unique_sections)
  testthat::expect_true(all(sections_found))
  
  # Check that key-value pairs are correctly associated with sections
  input_records <- dplyr::filter(result, section_name == "Input")
  if (nrow(input_records) > 0) {
    testthat::expect_true("Source" %in% input_records$key)
    testthat::expect_true("Options" %in% input_records$key)
  }
  
  sample_records <- dplyr::filter(result, section_name == "Sample")
  if (nrow(sample_records) > 0) {
    testthat::expect_true("Project" %in% sample_records$key)
    testthat::expect_true("SampleId" %in% sample_records$key)
  }
})

testthat::test_that("PID functions handle edge cases gracefully", {
  # Test with empty metadata
  testthat::expect_warning(
    empty_result <- ecotaxaLoadR:::parse_metadata_sections_long(
      character(0), 
      "test_scan", 
      "test_file.pid"
    ),
    "SampleId missing"
  )
  
  testthat::expect_s3_class(empty_result, "data.frame")
  testthat::expect_equal(nrow(empty_result), 1)  # Should have filename record
  testthat::expect_equal(empty_result$key[1], "filename")
  testthat::expect_true("sample_id" %in% names(empty_result))
  testthat::expect_true(all(is.na(empty_result$sample_id)))
  
  # Test with only section headers, no key-value pairs
  headers_only <- c("[Section1]", "[Section2]")
  testthat::expect_warning(
    headers_result <- ecotaxaLoadR:::parse_metadata_sections_long(
      headers_only, 
      "test_scan", 
      "test_file.pid"
    ),
    "SampleId missing"
  )
  
  testthat::expect_s3_class(headers_result, "data.frame")
  testthat::expect_equal(nrow(headers_result), 1)  # Should only have filename record
  testthat::expect_true(all(is.na(headers_result$sample_id)))
})

testthat::test_that("parse_metadata_sections_long warns and sets sample_id NA when SampleId is missing", {
  metadata_lines <- c(
    "PID",
    "[Sample]",
    "Project= hypoxia",
    "Date= 20240504-2248"
  )

  testthat::expect_warning(
    result <- ecotaxaLoadR:::parse_metadata_sections_long(
      metadata_lines,
      "test_scan",
      "missing_sample_id.pid"
    ),
    "SampleId missing"
  )

  testthat::expect_true("sample_id" %in% names(result))
  testthat::expect_true(all(is.na(result$sample_id)))
  testthat::expect_false(any(result$section_name == "Sample" & result$key == "SampleId"))
})

# NEW TESTS FOR SAMPLE FIELD PARSING

testthat::test_that("parse_sample_fields extracts cruise, moc, and net from SampleId", {
  # Create test metadata with SampleId
  metadata_records <- tibble::tibble(
    scan_id = "test_scan",
    section_name = c("Sample", "Sample", "Image"),
    key = c("SampleId", "Project", "Scanning_date"),
    value = c("sr2407_m6_n3", "hypoxia", "20250701_1741")
  )
  
  result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
  
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  
  # Check that cruise, moc, net are extracted
  parsed_keys <- result$key
  testthat::expect_true("cruise" %in% parsed_keys)
  testthat::expect_true("moc" %in% parsed_keys)
  testthat::expect_true("net" %in% parsed_keys)
  
  # Check values are correct
  cruise_value <- result$value[result$key == "cruise"]
  moc_value <- result$value[result$key == "moc"]
  net_value <- result$value[result$key == "net"]
  
  testthat::expect_equal(cruise_value, "sr2407")
  testthat::expect_equal(moc_value, "6")   # Just the number, not "m6"
  testthat::expect_equal(net_value, "3")   # Just the number, not "n3"
  
  # Check section_name is correct
  testthat::expect_true(all(result$section_name == "parsed_sample"))
})

testthat::test_that("parse_sample_fields extracts and converts Date field", {
  # Create test metadata with Date field
  metadata_records <- tibble::tibble(
    scan_id = "test_scan",
    section_name = c("Sample", "Sample"),
    key = c("Date", "Project"),
    value = c("20240504-2248", "test_project")
  )
  
  result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
  
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true("sample_date" %in% result$key)
  
  # Check date conversion
  sample_date_value <- result$value[result$key == "sample_date"]
  testthat::expect_equal(sample_date_value, "2024-05-04")
})

testthat::test_that("parse_sample_fields handles unmatched SampleId patterns", {
  # Create test metadata with invalid SampleId format
  metadata_records <- tibble::tibble(
    scan_id = "test_scan",
    section_name = "Sample",
    key = "SampleId",
    value = "invalid_format_123"
  )
  
  result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
  
  # Should return empty tibble for unmatched patterns
  testthat::expect_equal(nrow(result), 0)
})

testthat::test_that("parse_sample_fields handles invalid Date formats", {
  # Create test metadata with invalid Date format
  metadata_records <- tibble::tibble(
    scan_id = "test_scan",
    section_name = "Sample",
    key = "Date",
    value = "invalid-date-format"
  )
  
  result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
  
  # Should handle invalid dates gracefully
  date_records <- result[result$key == "sample_date", ]
  testthat::expect_equal(nrow(date_records), 0)
})

testthat::test_that("parse_sample_fields works with different SampleId patterns", {
  # Test different valid SampleId formats
  test_cases <- list(
    list(sample_id = "sr2407_m6_n3", expected = list(cruise = "sr2407", moc = "6", net = "3")),
    list(sample_id = "ab1234_m13_n8", expected = list(cruise = "ab1234", moc = "13", net = "8")),
    list(sample_id = "test2025_m1_n15", expected = list(cruise = "test2025", moc = "1", net = "15"))
  )
  
  purrr::walk(test_cases, function(test_case) {
    metadata_records <- tibble::tibble(
      scan_id = "test_scan",
      section_name = "Sample",
      key = "SampleId",
      value = test_case$sample_id
    )
    
    result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
    
    if (nrow(result) > 0) {
      cruise_value <- result$value[result$key == "cruise"]
      moc_value <- result$value[result$key == "moc"]
      net_value <- result$value[result$key == "net"]
      
      testthat::expect_equal(cruise_value, test_case$expected$cruise)
      testthat::expect_equal(moc_value, test_case$expected$moc)   # Just numbers
      testthat::expect_equal(net_value, test_case$expected$net)   # Just numbers
    } else {
      testthat::fail(paste("Failed to parse SampleId:", test_case$sample_id))
    }
  })
})

testthat::test_that("parse_sample_fields handles multiple date formats", {
  # Test different valid date formats
  test_cases <- list(
    list(date_input = "20240504-2248", expected = "2024-05-04"),
    list(date_input = "20231225-1200", expected = "2023-12-25"),
    list(date_input = "20250101-0000", expected = "2025-01-01")
  )
  
  purrr::walk(test_cases, function(test_case) {
    metadata_records <- tibble::tibble(
      scan_id = "test_scan",
      section_name = "Sample",
      key = "Date",
      value = test_case$date_input
    )
    
    result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
    
    if (nrow(result) > 0 && "sample_date" %in% result$key) {
      date_value <- result$value[result$key == "sample_date"]
      testthat::expect_equal(date_value, test_case$expected)
    } else {
      testthat::fail(paste("Failed to parse Date:", test_case$date_input))
    }
  })
})

testthat::test_that("parse_sample_fields handles combined SampleId and Date", {
  # Test with both SampleId and Date in the same metadata
  metadata_records <- tibble::tibble(
    scan_id = rep("test_scan", 4),
    section_name = c("Sample", "Sample", "Sample", "Image"),
    key = c("SampleId", "Date", "Project", "Scanning_date"),
    value = c("sr2408_m13_n8", "20240504-1530", "test_project", "20250701_1741")
  )
  
  result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
  
  testthat::expect_s3_class(result, "data.frame")
  testthat::expect_true(nrow(result) > 0)
  
  # Should have all 4 parsed fields: cruise, moc, net, sample_date
  expected_keys <- c("cruise", "moc", "net", "sample_date")
  parsed_keys <- result$key
  
  for (key in expected_keys) {
    testthat::expect_true(key %in% parsed_keys, 
                         info = paste("Missing key:", key))
  }
  
  # Check specific values
  testthat::expect_equal(result$value[result$key == "cruise"], "sr2408")
  testthat::expect_equal(result$value[result$key == "moc"], "13")     # Just the number
  testthat::expect_equal(result$value[result$key == "net"], "8")      # Just the number
  testthat::expect_equal(result$value[result$key == "sample_date"], "2024-05-04")
})

testthat::test_that("Integration: PID files with SampleId get parsed sample fields", {
  # Test that real PID file parsing includes parsed sample fields
  pid_file <- testthat::test_path("test-data", "pid_files", "sr2407_m6_n3_d2_b_1_dat1.pid")
  
  result <- ecotaxaLoadR:::parse_pid_file(pid_file)
  
  metadata <- result$image_metadata
  
  # Check if the file contains SampleId - if so, should have parsed fields
  sample_id_present <- any(metadata$section_name == "Sample" & metadata$key == "SampleId")
  
  if (sample_id_present) {
    sample_id_values <- unique(metadata$sample_id)
    testthat::expect_length(sample_id_values, 1)
    testthat::expect_equal(sample_id_values, metadata$value[metadata$section_name == "Sample" & metadata$key == "SampleId"][1])

    # Should have parsed sample fields
    testthat::expect_true(any(metadata$section_name == "parsed_sample"))
    
    parsed_fields <- metadata[metadata$section_name == "parsed_sample", ]
    expected_keys <- c("cruise", "moc", "net")
    
    for (key in expected_keys) {
      testthat::expect_true(key %in% parsed_fields$key, 
                           info = paste("Missing parsed field:", key))
    }
    
    # Check that cruise matches filename pattern
    cruise_value <- parsed_fields$value[parsed_fields$key == "cruise"]
    testthat::expect_true(grepl("sr2407", cruise_value))
  }
})

testthat::test_that("Integration: complete workflow includes all parsed fields", {
  # Test complete workflow from directory loading
  test_dir <- testthat::test_path("test-data", "pid_files")
  
  result <- ecotaxaLoadR::load_pid_files(test_dir)
  
  metadata <- result$metadata
  
  # Check if any files had SampleId fields
  sample_files <- any(metadata$section_name == "Sample" & metadata$key == "SampleId")
  
  if (sample_files) {
    testthat::expect_true("sample_id" %in% names(metadata))

    # Should have parsed sample fields
    testthat::expect_true(any(metadata$section_name == "parsed_sample"))
    
    parsed_records <- metadata[metadata$section_name == "parsed_sample", ]
    
    # Should have parsed fields for files with SampleId
    possible_keys <- c("cruise", "moc", "net", "sample_date")
    present_keys <- unique(parsed_records$key)
    
    # At least some keys should be present
    testthat::expect_true(length(intersect(possible_keys, present_keys)) > 0)
  }
})

testthat::test_that("parse_sample_fields pattern extensibility", {
  # This test verifies the pattern system is set up correctly for future extensions
  metadata_records <- tibble::tibble(
    scan_id = "test_scan",
    section_name = "Sample",
    key = "SampleId",
    value = "sr2407_m6_n3"
  )
  
  result <- ecotaxaLoadR:::parse_sample_fields(metadata_records, "test_scan")
  
  # Should work with current pattern
  testthat::expect_true(nrow(result) > 0)
  
  # Test that the function structure supports adding new patterns
  # (This is more of a code structure test)
  testthat::expect_true("cruise" %in% result$key)
  testthat::expect_true("moc" %in% result$key)
  testthat::expect_true("net" %in% result$key)
})