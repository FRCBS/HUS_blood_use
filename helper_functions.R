# helper functions v.0.0.1

###############################################
############# SETUP FUNCTION(S) ###############
###############################################
setup <- function(install_py = TRUE, set_CRAN = TRUE){
  ### Deprecated: Used only in research/blood_demand_v1/data_summary_reports/unfiltered_reports/quarto_documents/data_summaries_part1/Summaries_with_plots.qmd
  ### Esa's solution is currently in use and works better.
  # Set CRAN mirror
  if(set_CRAN){
    r <- getOption('repos')
    r['CRAN'] <- "https://ftp.acc.umu.se/mirror/CRAN/"
    options(repos=r)
  }  
  
  # Install and import the required Python packages and their dependencies
  if(install_py){
    if(!py_module_available("pandas")){
      py_install("pandas=1.4.4")
    }
    if(!py_module_available("fastparquet")){
      py_install("fastparquet")
    }
    pandas <- import("pandas")
    return(pandas)
  }
  return(NULL)
}

silence_pandas <- function(){
  # Function to silence pandas=1.4.4 FutureWarnings
  # if they appear.
  require(reticulate)
  warnings <- import("warnings")
  warnings$simplefilter(action="ignore", category="FutureWarning")
}

###############################################
########## DATA READING FUNCTIONS #############
###############################################
read_snappy_parquet <- function(path_to_dir, start=NULL, end=NULL){
  # NOTE! There exists an optimized version of the data reading function (see read_snappy_parquet_optimized()).
  # It is recommended to use that function, as it is 4 times faster.
  # When reading large files it is suggested to reduce the number of cores used.

  # Function that reads end-start number of snappy.parquet files from a given directory, 
  # starting from file index start and ending to index end.
  
  # Inputs: 
  #       path_to_dir: path to the target dir as a string
  #       start (optional): index of the first snappy.parquet file to be read in the target dir
  #       end (optional): index of the last snappy.parquet file to be read in the target dir
  # Outputs:
  #       combined_data_table: a tibble of containing all the data from the given snappy.parquet files
  
  # Direct dependencies: reticulate, dplyr (R), pandas 1.4.4 and fastparquet (Python) through reticulate
  
  # List all the Snappy-compressed Parquet files in the directory
  parquet_files <- list.files(path_to_dir, pattern = "\\.parquet$", full.names = TRUE)
  n_files <- length(parquet_files) # Number of files in the directory
  if(!n_files){stop("Reading file paths is not succeeding.")}
  
  # Check input parameters start and end. If out of range or not given, 
  # set start to 1 and end to the number of files in the folder.
  if(is.null(start) || start < 1 || start > n_files){
    start = 1
  }
  if(is.null(end) || end < 1 || end > n_files){
    end = n_files
  }
  
  # Initialize an empty list to store the data tables
  parquet_data_tables <- list()

  # Iterate through each Snappy-compressed Parquet file and read it using pandas with fastparquet dependency
  for (file in parquet_files[start:end]) {
    parquet_data <- pandas$read_parquet(file) %>%
      as_tibble()
    parquet_data <- suppressWarnings(unnest(parquet_data, keep_empty = TRUE))
    if(!length(parquet_data)){stop("Reading snappy.parquet file not working.")}
    parquet_data_tables <- append(parquet_data_tables, list(parquet_data))
    if(!length(parquet_data_tables)){stop("Appending parquet files not succeeding.")}
  }
  
  # Combine all the data tables into a single data table
  combined_data_table <- bind_rows(parquet_data_tables)
}

########################################################################################################################################
read_snappy_parquet_optimized <- function(path_to_dir, start = NULL, end = NULL, num_cores = NULL) {
  # Function that reads end-start number of snappy.parquet files from given directory, 
  # starting from file index start and ending to index end.
  
  # Inputs: 
  #       path_to_dir: path to the target dir as a string
  #       start (optional): index of the first snappy.parquet file to be read in the target dir.
  #                         If NULL, 1 by default.
  #       end (optional): index of the last snappy.parquet file to be read in the target dir.
  #                       If NULL, n_files by default.
  #       num_cores (optional): number of CPU cores to use in paralleled file reading process. 
  #                             If NULL, using the maximum number of cores found from the machine.
  # Outputs:
  #       combined_data_table: a tibble of containing all the data from the given nappy.parquet files
  
  # New dependencies: data.table, doParallel, future.apply, (foreach, parallel)
  
  # Set number of cores to use
  if (is.null(num_cores)){
    num_cores <- detectCores()
  }
  registerDoParallel(cores = num_cores)
  
  # List all the Snappy-compressed Parquet files in the directory
  parquet_files <- list.files(path_to_dir, pattern = "\\.parquet$", full.names = TRUE)
  n_files <- length(parquet_files)
  if(!n_files){stop("Reading file paths is not succeeding.")}
  
  # Set start to 1 and end to the number of files in the folder, if start or end not specified.
  if (is.null(start) || start < 1 || start > n_files) {
    start <- 1
  }
  if (is.null(end) || end < 1 || end > n_files) {
    end <- n_files
  }
  
  # Read each Snappy-compressed Parquet file parallelized using pandas with fastparquet dependency.
  parquet_data_tables <- foreach(file = parquet_files[start:end], 
                                 .packages = c("data.table", "pryr")) %dopar% {
    parquet_data <- suppressWarnings(pandas$read_parquet(file))
    suppressWarnings(unnest(parquet_data, keep_empty = TRUE))
  }
  
  combined_data_table <- rbindlist(parquet_data_tables) %>% 
    as_tibble()
}

########################################################################################################################################
read_data_according_to_variable <- function(path_to_dir, variable_name, var_list, num_cores = 4){
  # Function that reads only the rows from a given folder, where the column "variable_name" contains a variable present in "var_list".
  # Can be used to, e.g., filter the data according to ID. Especially useful when handling large data folders and the
  # complete data tables cannot be handled in RAM at once.
  
  # Inputs: 
  #       path_to_dir: path to target directory as a string
  #       variable_name: name of the variable according to which we are filtering
  #       var_list: list of variable realizations according to which the data is extracted
  # Outputs:
  #       a tibble of data filtered by rows according to the given variable specs
  
  # Define how files are read. Folders with a lot of files (>20) are read ~20 files at a time to prevent RAM memory from filling up.
  files <- list.files(path_to_dir, pattern = "\\.parquet$", full.names = TRUE)
  n_files <- length(files) # Number of files in the directory
  
  n_subsets <- max(ceiling(n_files/20), 1)
  var_data <- c()
  for (subset_i in 1:n_subsets){
    start <- ((subset_i - 1) * 20) + 1
    end <- min(subset_i * 20, n_files)
    subset_data <- read_snappy_parquet_optimized(path_to_dir, start, end, num_cores = num_cores)
    subset_data <- subset_data %>%
      filter_at(vars({{variable_name}}), any_vars(. %in% var_list))
    var_data <- bind_rows(var_data, subset_data)
  }
  return(var_data)
}

########################################################################################################################################
read_subset_of_variables <- function(path_to_dir, variable_names, num_cores = 4, num_subset_files = 20) {
  # This function reads only the column(s) specified by variable_names from a given data folder.
  # Especially useful when handling large data folders, that cannot be read to RAM completely.
  
  # Inputs: 
  #       path_to_dir: path to target directory as a string
  #       variable_names: names of the target variables/columns to read. Can be a string if one variable, or a vector of strings if multiple variables.
  #       num_cores (optional): number of CPU cores to use in paralleled file reading process. 4 by default.
  #       num_subset_files (optional): number of files to be read at one iteration. Note that with large folders and large num_cores, increasing may lead to RAM memory overflow.
  # Outputs:
  #       a tibble of data filtered by columns according to the given variable specs
  
  ## IMPORTANT NOTE ##
  # This function has not built-in method to monitor RAM state. 
  # Thus, if data_folder is suspected to be very large (>50 MB per file, hundreds of files), it is suggested to lower num_cores to e.g. 2.
  
  files <- list.files(path_to_dir, pattern = "\\.parquet$", full.names = TRUE)
  n_files <- length(files)
  
  n_subsets <- max(ceiling(n_files/num_subset_files), 1)
  subsets <- lapply(1:n_subsets, function(subset_i) {
    start <- ((subset_i - 1) * num_subset_files) + 1
    end <- min(subset_i * num_subset_files, n_files)
    
    subset_data <- read_snappy_parquet_optimized(path_to_dir, start, end, num_cores = num_cores)
    
    if (is.character(variable_names)) {
      subset_data <- subset_data %>% 
        select({{variable_names}})
    } else if (is.vector(variable_names) | is.list(variable_names)) {
      subset_data <- subset_data %>%
        select(all_of(variable_names))
    }
    
    return(subset_data)
  })
  
  col_data <- dplyr::bind_rows(subsets)
  return(col_data)
}


########################################################################################################################################
######## Data summary functions ########################################################################################################
########################################################################################################################################

summarize_data <- function(path_to_dir = NULL, table = NULL, is_large_table = FALSE, keep_outliers = FALSE, 
                           skip_variables = c("henkilotunnus"), include_only_variables = NULL, summarize_on_individual_level = FALSE){
  # Function that summarizes the data in given folder. Latest version: 15.7.2023
  
  # Inputs: 
  #       path_to_dir: Path to target directory as a string, or the data from the dir as a tibble
  #       table: If instead of a path a table is summarized, the table can be given with this argument.
  #       is_large_table: reading large files is handled column at a time.
  #       keep_outliers (omitted for now/not implemented!): Boolean specifying, if summary should be filtered to exclude categories or numeric values with less than 5 occurrences. FALSE by default.
  #       skip_variables: skip summarizing these variables
  #       summarize_on_individual_level: Whether to calculate frequencies of categorical variables so that only unique observations are counted per individual.
  # Outputs:
  #       ... frequency tables, histograms, variable names, min, median, max etc.
  if (is.null(table)){
    if (is_large_table) {
      data <- read_snappy_parquet_optimized(path_to_dir, 1, 1)
      if (object_size(data) < 30e6){
        n_cores <- 2
      } else {
        n_cores <- 1
      }
    } else {
      data <- read_snappy_parquet_optimized(path_to_dir)
    }
  } else {
    data <- table
  }
  
  
  # Print what folder is summarized
  if (!is.null(path_to_dir)){
    dirs <- unlist(strsplit(path_to_dir, "/"))
    dir_name <- dirs[length(dirs)]
  } else {
    dir_name <- "Table"
  }
  
  
  # Print what variables are in the folder
  cat("\nVariables in", dir_name, ":\n\n")
  col_names <- names(data)
  print(col_names)

  if (summarize_on_individual_level & is_large_table & !is.null(path_to_dir)){
    ID_col <- read_subset_of_variables(path_to_dir, "henkilotunnus")
  }
  
  if(!is.null(include_only_variables)){
    col_names <- include_only_variables
  }
  
  if (length(col_names) == 1){
    if (col_names == "henkilotunnus"){
      cat("\nTable has only unique IDs.\n")
      return(invisible())
    }
  }
  # Print summaries of variables specified by input arguments
  cat("\nSummaries of selected variables:\n")
  print(setdiff(col_names, skip_variables))
  for(col_name in setdiff(col_names, skip_variables)){
    # The data in the tibble column
    if (is_large_table) {
      col_data <- read_subset_of_variables(path_to_dir, col_name, n_cores)
      if (summarize_on_individual_level){
        col_data <- bind_cols(ID_col, col_data)  
      }
    } else {
      col_data <- data %>% 
        select(c(henkilotunnus, {{col_name}}))
    }
    # Get type of summary (categorical, numerical, emptyColumn, date)
    # Change "NULL" and empty values to NA
    col_data[[col_name]] <- missing_to_NA(col_data[[col_name]])
    # Summarize the data according to class
    if (is.character(col_data[[col_name]]) | is.factor(col_data[[col_name]])){
      if(check_if_numeric(col_data[[col_name]])){
        the_grobs <- summarize_numerical(col_data[,col_name], keep_outliers)
      } else {
        the_grobs <- summarize_categorical(col_data[,col_name], keep_outliers)
        if (summarize_on_individual_level){
          indv_grobs <- summarize_categorical_individual(col_data, col_name, keep_outliers)
          grid.arrange(grobs = indv_grobs, ncol = 2, dpi = 50)
        }
      }

    }
    if (is.logical(col_data[[col_name]])){
      the_grobs <- summarize_logical(col_data[,col_name], keep_outliers)
    } 
    if (is.POSIXct(col_data[[col_name]]) | is.POSIXlt(col_data[[col_name]]) | is.POSIXt(col_data[[col_name]]) | is.Date(col_data[[col_name]])){
      the_grobs <- summarize_date(col_data[,col_name], keep_outliers)
    } 
    if (is.numeric(col_data[[col_name]]) | all(is.na(col_data[[col_name]]))){
      the_grobs <- summarize_numerical(col_data[,col_name], keep_outliers)
    }
    grid.arrange(grobs = the_grobs, ncol = 2, dpi = 50)
  }
}

########################################################################################################################################
summarize_categorical <- function(column, 
                                  keep_outliers = TRUE, 
                                  print_max = 10, 
                                  is_numeric = FALSE,
                                  title_n = NULL,
                                  x_lab = NULL,
                                  num_letters = 6,
                                  flip_coord = FALSE,
                                  color = NULL){
  col_name <- names(column)
  cat("\n", col_name, "\n")
  if (is_numeric){
    print("Numeric")
  } else {
    print("Character")
  }
  
  # Summarize
  na_count <- sum(is.na(column[[col_name]]))
  n_row <- nrow(column)
  column <- column %>%
    group_by(!!sym(col_name)) %>%
    summarise(Frequency = n()) %>%
    arrange(desc(Frequency)) %>%
    filter(!is.na(!!sym(col_name)))
  
  if (nrow(column) > print_max){
    others_sum <- sum(column$Frequency[(print_max+1):nrow(column)])
    column <- column[1:(print_max-2),] %>% 
      add_row(!!sym(col_name) := "Others", Frequency = others_sum) %>%
      add_row(!!sym(col_name) := "NA's", Frequency = na_count)
  } 

  column[[col_name]] <- factor(column[[col_name]], levels = unique(column[[col_name]]))
  
  # Plot
  if (na_count == n_row){
    barplt <- tableGrob(matrix("All items are missing.", ncol = 1), theme = ttheme_minimal())
  } else {
    col_palette <- brewer.pal(12, "Set3")[-2]
    if (is.null(color)){
      color <- sample(col_palette, 1)
    }
    if (is.null(title_n)){title_n <- paste0(col_name, ": Ungrouped")}
    if (is.null(x_lab)){x_lab <- col_name}
    barplt <- ggplot(data=column, aes(x = !!sym(col_name), y = Frequency)) +
      geom_bar(stat = "identity", fill = c(color)) +
      labs(title = title_n, x = x_lab) +
      theme_minimal() +
      scale_x_discrete(labels = function(x) stringr::str_sub(x, end = num_letters)) +
      theme(axis.text.x = element_text(angle = 45, hjust = 1))
    if (flip_coord){
      barplt <- barplt + 
        scale_x_discrete(limits = rev(levels(column[[{{col_name}}]]))) +
        coord_flip()
        
    }
  }

  column <- column %>%
    add_row(!!sym(col_name) := "NA's", Frequency = na_count)
  column[[col_name]] <- str_sub(column[[col_name]], end=20)
  summ_grob <- tableGrob(as.data.frame(column), theme = ttheme_minimal())

  return(list(summ_grob, barplt))
  
}

########################################################################################################################################
summarize_numerical <- function(column, keep_outliers = TRUE){
  # If number of unique instances is less than 3, the variable is summarized as categorical.
  if (length(unique(column[[1]])) < 3){
    column[[1]] <- as.character(column[[1]])
    grob_list <- summarize_categorical(column, keep_outliers, is_numeric = TRUE)
    return(grob_list)

  } else {
    col_name <- colnames(column)
    cat("\n", col_name, "\n")
    print("Numeric")
    # Summarize
    column[[1]] <- as.numeric(column[[1]])
    summary_data <- summary(column)
    summ_grob <- tableGrob(matrix(summary_data), theme = ttheme_minimal())
    # Plot
    if (all(is.na(column[[1]]))){
      histogram <- tableGrob(matrix("All items are missing.", ncol = 1), theme = ttheme_minimal())
    } else {
      col_palette <- brewer.pal(12, "Set3")[-2]
      color <- sample(col_palette, 1)
      histogram <- ggplot(data=data.frame(column[[1]]), aes(x=column[[1]])) +
        geom_histogram(color=c(color), fill=c(color)) +
        labs(title = col_name, x = col_name) +
        theme_minimal()
    }
    return(list(summ_grob, histogram))
  }
}

########################################################################################################################################
summarize_date <- function(column, keep_outliers = TRUE){
  col_name <- names(column)
  cat("\n", col_name, "\n")
  print("Date")
  # Summarize
  summary_data <- summary(column)
  summ_grob <- tableGrob(matrix(summary_data), theme = ttheme_minimal())
  # Plot
  if (all(is.na(column[[1]]))){
    histogram <- tableGrob(matrix("All items are missing.", ncol = 1), theme = ttheme_minimal())
  } else {
    col_palette <- brewer.pal(12, "Set3")[-2]
    color <- sample(col_palette, 1)
    histogram <- ggplot(data=data.frame(column[[1]]), aes(x=column[[1]])) +
      geom_bar(binwidth = 30, color=c(color), fill=c(color)) +
      labs(title = col_name, x = col_name) +
      theme_minimal()
  }
  return(list(summ_grob, histogram))
}

########################################################################################################################################
summarize_categorical_individual <- function(column, col_name, keep_outliers = TRUE, print_max = 10){
  # Summarize
  na_count <- sum(is.na(column[[col_name]]))
  n_row <- nrow(column)
  
  column <- column %>%
    distinct() %>%
    group_by(!!sym(col_name)) %>%
    summarise(Frequency = n()) %>%
    arrange(desc(Frequency)) %>%
    filter(!is.na(!!sym(col_name)))
  
  all_ones <- all(column$Frequency == 1)
  
  if (nrow(column) > print_max){
    others_sum <- sum(column$Frequency[(print_max+1):nrow(column)])
    column <- column[1:(print_max-2),] %>% 
      add_row(!!sym(col_name) := "Others", Frequency = others_sum)
  } 
  
  column[[col_name]] <- factor(column[[col_name]], levels = unique(column[[col_name]]))
  
  # Plot
  if (na_count == n_row){
    barplt <- tableGrob(matrix("All items are missing.", ncol = 1), theme = ttheme_minimal())
  } else {
    if (all_ones){
      barplt <- tableGrob(matrix("All items are unique across individuals.", ncol = 1), theme = ttheme_minimal())
    } else {
      col_palette <- brewer.pal(12, "Set3")[-2]
      color <- sample(col_palette, 1)
      barplt <- ggplot(data=column, aes(x = !!sym(col_name), y = Frequency)) +
        geom_bar(stat = "identity", fill = c(color)) +
        labs(title = paste0(col_name, ": Grouped"), x = col_name) +
        theme_minimal() +
        scale_x_discrete(labels = function(x) stringr::str_sub(x, end = 6)) +
        theme(axis.text.x = element_text(angle = 45, hjust = 1))
    }
  }
  
  column <- column %>%
    add_row(!!sym(col_name) := "NA's", Frequency = na_count)
  
  column[[col_name]] <- str_sub(column[[col_name]], end=20)
  
  summ_grob <- tableGrob(as.data.frame(column), theme = ttheme_minimal())

  return(list(summ_grob, barplt))
}

########################################################################################################################################
summarize_logical <- function(column, keep_outliers = TRUE) {
  col_name <- names(column)
  cat("\n", col_name, "\n")
  print("Logical")
  # Summarize
  summary_data <- summary(column)
  summ_grob <- tableGrob(matrix(summary_data), theme = ttheme_minimal())
  # Plot
  if (all(is.na(column[[1]]))){
    barplt <- tableGrob(matrix("All items are missing.", ncol = 1), theme = ttheme_minimal())
  } else {
    col_palette <- brewer.pal(12, "Set3")[-2]
    color <- sample(col_palette, 1)
    barplt <- ggplot(data=column, aes(x=!!sym(col_name))) +
      geom_bar(color=c(color), fill=c(color)) +
      labs(title = col_name, x = col_name, y = "Frequency") +
      theme_minimal()
  }
  return(list(summ_grob, barplt))
}


########################################################################################################################################
missing_to_NA <- function(column){
  if (!(sample(class(column), 1) %in% c("POSIXct", "POSIXt", "Date", "POSIXlt"))){
    column[column=="NULL" | column==""] <- NA
  }
  return(column)
}

########################################################################################################################################
check_if_numeric <- function(column){
  if (is.POSIXct(column) | is.POSIXlt(column) | is.POSIXt(column) | is.Date(column) | is.logical(column)) {
    return(FALSE)
  }
  num_vals <- missing_to_NA(column)
  num_vals <- na.omit(num_vals)
  num_vals <- suppressWarnings(as.numeric(num_vals))
  if (any(is.na(num_vals))) {
    return(FALSE)
  }
  if (length(unique(num_vals)) < 10){
    return(FALSE)
  }
  return(TRUE)
}


########################################################## TEST ###########################################################################
#### This function worked well, but now, due to some environment change, the printed tables are not rendered correctly.####################
#### Problem has been tried to debug without success. Thus, utilizing dfSummary and it's concise outputs is not an option at the moment.###
#### UPDATE! It again started working - Some changes in HUS Acamedic? #####################################################################
###########################################################################################################################################
concise_data_summary <- function(path_to_dir = NULL,
                                 data_table = NULL,
                                 discard_ID = TRUE,
                                 remove_duplicates = TRUE, 
                                 is_large_table = FALSE,
                                 image_dir){
  # NOTE! If this function, or especially dfSummary() is used inside a quarto document, make sure to set 
  # '|# results: asis' to the beginning of the code block, so the output is rendered correctly."
  
  if (is.null(path_to_dir) & is.null(data_table)){stop("Either path_to_dir or data_table required.")}
  if (is_large_table & is.null(path_to_dir)){stop("Argument is_large_table = TRUE requires path_to_dir.")}
  
  # Summarizing small and medium sized tables
  if (!is_large_table){
    
    # Read data
    if (!is.null(data_table)){
      data <- data_table
      table_name <- deparse(substitute(data_table))
    } else {
      data <- read_snappy_parquet_optimized(path_to_dir, num_cores = 6)
      dirs <- unlist(strsplit(path_to_dir, "/"))
      table_name <- dirs[length(dirs)]
    }
    
    if (nrow(data) == 0){
      df_summary <- summary(data)
    } else {
      # Filter data according to provided arguments
      if (remove_duplicates){data <- data %>% distinct()}
      if (discard_ID){data <- data %>% select(-henkilotunnus)}
      
      # Print what variables are in the folder
      cat("\nVariables in", table_name, ":\n\n")
      col_names <- colnames(data)
      cat(col_names, "\n\n")
      
      cat("Summary of variables:\n\n")
      
      # Print summaries of variables specified by input arguments
      
      # First, convert numerical characters to numeric and missing values to NA
      for(col_name in col_names){
        col_data <- data[,col_name]

        if (check_if_numeric(col_data[[1]])){
          data[,col_name][[1]] <- as.numeric(data[,col_name][[1]])
        }
        # Convert "NULL" and empty values to NA
        col_data <- data[,col_name]
        col_data[[1]] <- missing_to_NA(col_data[[1]])
        data[,col_name] <- col_data
      }
      
      # Then, print the summaries
      df_summary <- dfSummary(data,
                                plain.ascii = FALSE, 
                                style="grid", 
                                valid.col = FALSE, 
                                graph.magnif = 1,
                                tmp.img.dir = image_dir)
      print(df_summary)
    }
  }
}
############################################################### TEST ENDS ####################################################################

########################################################################################################################################
######## Plotting functions ########################################################################################################
########################################################################################################################################

plot_top_n <- function(data, var, n = 20) {
  # Ensure the variable exists in the data frame
  if (!(var %in% names(data))) {
    stop(paste("Variable", var, "not found in data frame."))
  }
  
  total_rows <- nrow(data)
  na_percentage <- (sum(is.na(data[[var]])) / total_rows) * 100
  
  # Counting the frequency
  count_data <- as.data.frame(table(data[[var]]))
  
  # Naming the columns
  colnames(count_data) <- c(var, "freq")
  
  # Adding a percentage column
  count_data$perc <- (count_data$freq / total_rows) * 100
  
  # Sorting and filtering to keep only top n
  top_n_data <- count_data %>%
    arrange(desc(freq)) %>%
    head(n)
  
  # Plotting the bar graph
  p <- ggplot(top_n_data, aes_string(x = paste("reorder(", var, ", -freq)"), y = "freq")) +
    geom_bar(stat = "identity", fill = "steelblue") +
    geom_text(aes(y = freq, label = paste(round(perc, 2), "%")), vjust = -0.5) +
    coord_flip() +
    labs(
      title = paste("Top", n, var),
      subtitle = paste("Percentage of NAs:", round(na_percentage, 2), "%"),
      x = var,
      y = "Frequency"
    ) +
    theme_minimal()
  
  print(p)
}

########### Plotting transfusion distributions over events ##############################
# Transferred here from analyses_elissa.qmd in 21.5.2024

# No grouping
transfusionEventDistribution <- function(table, plot_title, 
                                        variable = "shjakso_numero", 
                                        save = FALSE, 
                                        path = NULL, 
                                        title= NULL, 
                                        fill_var_name = "episodes", 
                                        color_var_name = "RBC", 
                                        color_var_name_x = "red blood cell",
                                        bw = FALSE){

#, title= NULL, x= NULL, y = NULL, color = NULL, fill = NULL
# First we will summarize the number of transfusion events within variable
# and create categories for number of transfusion events (1,2,...,9,+10).
    variable_summary <- table %>%
        group_by(!!sym(variable)) %>%
        summarise(num_transf_within_shj = n()) %>%
        mutate(category_for_plot = ifelse(num_transf_within_shj >= 10, 
        "+10", as.character(num_transf_within_shj))) %>%
        mutate(category_for_plot = reorder(category_for_plot, -as.numeric(category_for_plot)))

    # Then we will summarize the shakso proportions as percentages, by grouping by 
    # the categories created above 
    variable_summary_density <- variable_summary %>%
        group_by(category_for_plot) %>%
        summarize(count = n()) %>%
        mutate(proportion_perc = count/sum(count)*100)

    # Finally, we'll calculate the cumulative proportion of transfusion events as percentages,
    # w.r.t the number of transfusions (categories created above). 
    transfusion_cumsum <- data.frame(
        category_for_plot  = as.character(c(1:9, "+10")),
        cumulat_prop = sapply(c(1:9, max(variable_summary$num_transf_within_shj, na.rm=TRUE)),
            function(x) sum(variable_summary$num_transf_within_shj[variable_summary$num_transf_within_shj <= x]/
                sum(variable_summary$num_transf_within_shj)*100))
    )

    # Then to the actual point! Here we will recreate the fig. 1 from Auvinen *et al.* (2020). Breakdown of the code is provided below.

    # First we'll plot the variable proportions.

    if (bw) {
      color1 <- gray(0.3)
      color2 <- gray(0.7)
    } else {
      color1 <- "violetred3"
      color2 <- "aquamarine3"
    }

    p <- ggplot(filter(variable_summary_density, count >= 5), aes(x = as.character(category_for_plot))) + # Define variable proportion data
        geom_bar(aes(y = proportion_perc, fill=color2), stat = "identity") + # plot the bars

        # Then we'll plot the cumulative proportion of transfusions w.r.t. number of transfusions.
        geom_point(data = transfusion_cumsum, aes(y = cumulat_prop, x = as.character(category_for_plot)), color= color1, group=1) + # Here we plot the points
        geom_text(data = transfusion_cumsum, aes(y = cumulat_prop, label = as.character(round(cumulat_prop, 1))), hjust = -0.1, vjust = 1.6) + # Here we add the percentage numbers text to the points
        geom_line(data = transfusion_cumsum, aes(y = cumulat_prop, x = as.character(category_for_plot), color=color1, group=1)) + # Here we add a line to go through the points

        # Then we'll flip x-axis since it was wrong way around
        scale_x_discrete(limits = rev(levels(variable_summary$category_for_plot))) + 

        # Basic label adding
        # labs(x = paste0(x), y = paste0(y))
        labs(y = paste0("Proportion of ", fill_var_name, " (%)"), x = paste0("Number of ", color_var_name_x, " transfusions")) +
        # ggtitle(paste0(plot_title, "\n" , 
                        # "Distribution of blood product transfusion events over ", variable)) +
        ggtitle((title)) +

        # And finally, add the legend and define the theme
        scale_color_manual(values = color1, labels = paste0("Cumulative proportion of\n", color_var_name, " transfusions (%)"), guide = guide_legend(title = NULL)) +
        # scale_color_manual(values = "violetred3", labels = color, guide = guide_legend(title = NULL)) +
        scale_fill_manual(values = color2, labels = paste0("Proportion of ", fill_var_name, " (%)"), guide = guide_legend(title = NULL)) +
        # scale_fill_manual(values = "aquamarine3", labels = paste0("Proportion of transfusion episodes (%)"), guide = guide_legend(title = NULL)) +
        # scale_fill_manual(values = "aquamarine3", labels = fill, guide = guide_legend(title = NULL)) +
        theme_minimal() +
        scale_y_continuous(expand=c(0.02,0.01)) +
        theme(legend.position="bottom",
        text = element_text(size = 13), 
        axis.text.x = element_text(size = 13),
        axis.text.y = element_text(size = 13),
        axis.title.x = element_text(margin = margin(t = 15)))

    if(save){ggsave(path, p)}
    return(p)
}

# Grouped
transfusionEventDistribution_Grouped <- function(table,
                                                plot_title,
                                                variable = NULL,
                                                grouping_variable = "sex_extended",
                                                save = FALSE,
                                                path = NULL,
                                                title=NULL,
                                                y_var_name = "episodes",
                                                fill_var_name = "episodes",
                                                color_var_name = "RBC",
                                                color_var_name_x = "red blood cell",
                                                grouping_var_name = "sex",
                                                fill_colors = NULL,
                                                color_colors = NULL){
    # First we will summarize the number of transfusion events within variable
    # and create categories for number of transfusion events (1,2,...,9,+10).
    variable_summary <- table %>%
        filter(!is.na(!!sym(grouping_variable)) & !is.na(!!sym(variable))) %>%
        group_by(!!sym(variable), !!sym(grouping_variable)) %>%
        # summarise(num_transf_within_shj = n()) %>% # Before ID count filtering
        mutate(num_transf_within_shj = n()) %>%
        select(!!sym(variable), !!sym(grouping_variable), num_transf_within_shj, henkilotunnus) %>%
        distinct() %>%
        mutate(category_for_plot = ifelse(num_transf_within_shj >= 10,
        "+10", as.character(num_transf_within_shj))) %>%
        mutate(category_for_plot = factor(category_for_plot))
    
    # Order factor levels correctly
    variable_summary$category_for_plot <- factor(variable_summary$category_for_plot, levels = c(paste0(1:9), "+10"))

    # Then we will summarize the variable proportions as percentages, by grouping by 
    # the categories created above 
    variable_summary_density <- variable_summary %>%
        group_by(!!sym(grouping_variable), category_for_plot) %>%
        summarise(
          count = n(),
          n_ID = n_distinct(henkilotunnus)
        ) %>%
        mutate(proportion_perc = count/sum(count)*100)

    # Finally, we'll calculate the cumulative proportion of transfusion events as percentages,
    # w.r.t the number of transfusions (categories created above). 
    groups <- unique(variable_summary[[grouping_variable]])

    if (length(groups) < 2 | length(groups) > 3){episode_
        stop("There has to be at least 2 groups and at most 3.")
    }

    s1 <- groups[1]
    s2 <- groups[2]

    variable_summary_s1 <- variable_summary %>% 
        filter(!!sym(grouping_variable) == s1) %>% 
        ungroup() %>%
        select(num_transf_within_shj) %>%
        pull()

    transfusion_cumsum_s1 <- data.frame(
        category_for_plot  = as.character(c(1:9, "+10")),
        cumulat_prop = sapply(c(1:9, max(variable_summary_s1, na.rm=TRUE)),
            function(x) sum(variable_summary_s1[variable_summary_s1 <= x]/
                sum(variable_summary_s1)*100))
    )

    variable_summary_s2 <- variable_summary %>% 
        filter(!!sym(grouping_variable) == s2) %>% 
        ungroup() %>%
        select(num_transf_within_shj) %>%
        pull()

    transfusion_cumsum_s2 <- data.frame(
        category_for_plot  = as.character(c(1:9, "+10")),
        cumulat_prop = sapply(c(1:9, max(variable_summary_s2, na.rm=TRUE)),
            function(x) sum(variable_summary_s2[variable_summary_s2 <= x]/
                sum(variable_summary_s2)*100))
    )

    transfusion_cumsum_s1[[grouping_variable]][1:nrow(transfusion_cumsum_s1)] <- as.character(s1)
    transfusion_cumsum_s2[[grouping_variable]][1:nrow(transfusion_cumsum_s2)] <- as.character(s2)

    transfusion_cumsum <- bind_rows(transfusion_cumsum_s1,
                                    transfusion_cumsum_s2)

    if (length(groups) == 3){
        s3 <- groups[3]
        variable_summary_s3 <- variable_summary %>% 
            filter(!!sym(grouping_variable) == s3) %>% 
            ungroup() %>%
            select(num_transf_within_shj) %>%
            pull()

        transfusion_cumsum_s3 <- data.frame(
            category_for_plot  = as.character(c(1:9, "+10")),
            cumulat_prop = sapply(c(1:9, max(variable_summary_s3, na.rm=TRUE)),
                function(x) sum(variable_summary_s3[variable_summary_s3 <= x]/
                    sum(variable_summary_s3)*100))
        )
        transfusion_cumsum_s3[[grouping_variable]][1:nrow(transfusion_cumsum_s3)] <- as.character(s3)
        transfusion_cumsum <- bind_rows(transfusion_cumsum, 
                                    transfusion_cumsum_s3)
    }

    transfusion_cumsum[[grouping_variable]] <- factor(transfusion_cumsum[[grouping_variable]], 
                                                      levels = levels(groups))

    # Plotting

    # Define plot colours
    if (is.null(fill_colors) | is.null(color_colors)){
        # colors1 <- c("aquamarine3", "pink2", "salmon") # barplots
        colors1 <- c("violet", "turquoise")

        # colors2 <- c("dodgerblue2", "maroon2", "coral3") # scatter & line
        colors2 <- c("violetred", "turquoise3")

        if (length(groups) == 3){
            colors1 <- append(colors1, "salmon")
            colors2 <- append(colors2, "tomato2")
        }
    } else {
        colors1 <- fill_colors
        colors2 <- color_colors 
    }
    
    # Added this
    join_tibble <- transfusion_cumsum %>%
        full_join(variable_summary_density, by=intersect(names(transfusion_cumsum), 
                                                          names(variable_summary_density)))
    join_tibble$category_for_plot <- factor(join_tibble$category_for_plot, levels = c(paste0(1:9), "+10"))
    
    # p <- ggplot(filter(variable_summary_density, count >= 5), aes(x = category_for_plot, y = proportion_perc)) + # Define variable proportion data
    p <- ggplot(filter(join_tibble, n_ID >= 5), aes(x = category_for_plot, y = proportion_perc)) + # Define variable proportion data
        geom_bar(aes(fill=!!sym(grouping_variable)), stat = "identity", position = position_dodge2(preserve = 'single'), alpha=0.8) + # plot the bars

        # Then we'll plot the cumulative proportion of transfusions w.r.t. number of transfusions.
        # geom_point(data = transfusion_cumsum, aes(y = cumulat_prop, x = category_for_plot, color=!!sym(grouping_variable))) + # Here we plot the points
        # geom_text(data = transfusion_cumsum, aes(y = cumulat_prop, label = as.character(round(cumulat_prop, 1))), hjust = 1.2, vjust = 0) + # Here we add the percentage numbers text to the points
        # geom_line(data = transfusion_cumsum, aes(y = cumulat_prop, x = category_for_plot, color=!!sym(grouping_variable), group=!!sym(grouping_variable))) + # Here we add a line to go through the points
        geom_point(aes(y = cumulat_prop, x = category_for_plot, color=!!sym(grouping_variable))) + # Here we plot the points
        geom_line(aes(y = cumulat_prop, x = category_for_plot, color=!!sym(grouping_variable), group=!!sym(grouping_variable))) + # Here we add a line to go through the points
        geom_text(aes(y = cumulat_prop, label = as.character(round(cumulat_prop, 1))), hjust = 0.05, vjust = 1.6) + # Here we add the percentage numbers text to the points

        # Basic label adding
        labs(y = paste0("Proportion of ", y_var_name, " (%)"), x = paste0("Number of ", color_var_name_x, " transfusions")) +
        # ggtitle(paste0(plot_title, "\n" , 
                        # "Distribution of blood product transfusion events over ", variable)) +
        ggtitle((title)) +

        # And finally, add the legend and define the theme
        scale_fill_manual(values = colors1, name = paste0("Proportion of ", fill_var_name, " by\n", grouping_var_name, " (%)")) +
        scale_color_manual(values = colors2, name = paste0("Cumulative proportion of\n", color_var_name, " transfusions\nby ", grouping_var_name, " (%)")) +
        scale_y_continuous(expand = c(0.01,0.01)) +
        theme_minimal() +
        theme(
          text = element_text(size = 12),
          axis.text.x = element_text(size = 12),
          axis.text.y = element_text(size = 12),
          axis.title.x = element_text(margin = margin(t = 14))
        ) +
        guides(fill = guide_legend(order = 1), color = guide_legend(order = 2))

    if(save){ggsave(path, p)}
    return(p)
}


########################################################################################################################################
######## Other ########################################################################################################
########################################################################################################################################


num_individuals <- function(data_tibble){
  length(unique(data_tibble[["henkilotunnus"]]))
}


slice_agegroup <- function(df, a = NULL, b = NULL) {
    # assumes existence of column 'potilas_ika_vuosia'
    if (is.null(a) & is.null(b)) {
        print("Please provide at least one boundary.")
        return()
    }
    if (is.null(a)) {
        out <- df %>% filter(potilas_ika_vuosia <= b)
        return(out)
    }
    if (is.null(b)) {
        out <- df %>% filter(potilas_ika_vuosia >= a)
        return(out)
    }
    if (!is.null(a) & !is.null(b)) {
        out <- df %>% filter(potilas_ika_vuosia >= a & potilas_ika_vuosia <= b)
        return(out)
    }
}

# Esa's smaller viewer function because VSCode R loads the entire df immediately
# (RStudio loads rows in lazily)
v <- function(df) {View(df[1:2000, ])}

# Function to investigate and print out statistics on tranfusion blood type hierarchies,
# i.e. how many transfusions have RBC-wise logical individual blood types and unit blood types.
check_blood_type_hierarchy <- function(BT_transf_fj_table) {
    # Assuming the input table has columns ABO and RhD to describe 
    # the mapped blood types of an individual, and columns ABO_unit 
    # and RhD_unit to describe the blood types of transfused ABO_A_unit

    # ABO hierarchy
    O_or_NA_receives = c("O")
    A_receives = c(O_or_NA_receives, "A")
    B_receives = c(O_or_NA_receives, "B")
    AB_receives = c(A_receives, B_receives, "AB")

    # RhD hierarchy
    neg_or_NA_receives = c("neg")
    pos_receives = c(neg_or_NA_receives, "pos")

    # Create boolean columns to tell whether conditions are satisfied
    BT_transf_mutated <- BT_transf_fj_table %>%
        mutate(
            correct_ABO = ifelse(
                is.na(ABO) & !(ABO_unit %in% O_or_NA_receives), FALSE, ifelse(
                    (ABO %in% c(NA, "O") & ABO_unit %in% O_or_NA_receives), TRUE, ifelse(
                        (ABO == "A" & ABO_unit %in% A_receives), TRUE, ifelse(
                            (ABO == "B" & ABO_unit %in% B_receives), TRUE, ifelse(
                                (ABO == "AB" & ABO_unit %in% AB_receives), TRUE, FALSE
                            )
                        )
                    )
                )
            ),
            correct_RhD = ifelse(
                is.na(RhD) & !(RhD_unit %in% neg_or_NA_receives), FALSE, ifelse(
                    (RhD %in% c(NA, "neg") & RhD_unit %in% neg_or_NA_receives), TRUE, ifelse(
                        (RhD == "pos" & RhD_unit %in% pos_receives), TRUE, FALSE
                    )
                )
            )
        )

    # Summarise the results
    BT_transf_mutated %>%
        group_by(product_type) %>%
        summarise(
            # The mapping is RBC wise just as it should
            RBCwise_correct_ABO_and_RhD = sum(correct_ABO & correct_RhD),
            RBCwise_correct_ABO_and_RhD_frac = RBCwise_correct_ABO_and_RhD/n(),
            # The mapping is RBC wise correct for ABO but not RhD
            RBCwise_correct_ABO_incorrect_RhD = sum(correct_ABO & !correct_RhD),
            RBCwise_correct_ABO_incorrect_RhD_frac = RBCwise_correct_ABO_incorrect_RhD/n(),
            # RBC wise correct for RhD but not ABO
            RBCwise_incorrect_ABO_correct_RhD = sum(!correct_ABO & correct_RhD),
            RBCwise_incorrect_ABO_correct_RhD_frac = RBCwise_incorrect_ABO_correct_RhD/n(),
            # Both are wrong
            RBCwise_incorrect_ABO_and_RhD = sum(!correct_ABO & !correct_RhD),
            RBCwise_incorrect_ABO_and_RhD_frac = RBCwise_incorrect_ABO_and_RhD/n(),
            # Both are wrong but veri_admin_type is NA (i.e. ABO_unit & RhD_unit are NA)
            RBCwise_incorrect_ABO_and_RhD_and_units_NA = sum(!correct_ABO & !correct_RhD & is.na(ABO_unit) & is.na(RhD_unit)),
            RBCwise_incorrect_ABO_and_RhD_and_units_NA_frac = RBCwise_incorrect_ABO_and_RhD_and_units_NA/n(),
            # Both individual blood types and unit blood types are NA 
            All_blood_types_NA = sum(is.na(ABO) & is.na(RhD) & is.na(ABO_unit) & is.na(RhD_unit)),
            All_blood_types_NA_frac = All_blood_types_NA/n()
        )
}

patient_timeline <- function(ID = NA) {
    
    real_kaynti <- c("Ensikäynti", "Sarjahoitokäynti", "Lisäkäynti", "Päivystys",
                     "Hoitokäynti", "Tk-päivystys", "Päiväkirurgia", "Seulontakäynti", 
                     "Muualle ohjattu", "Kotikäynti (esh)", "Osastopotilaan konsultaatio",
                     "Päiväsairaanhoito", "Tk-hoitokäynti, hoitaja", "Tutkimuskäynti/psyk.", "Tk-Viranomaispalvelu", "Tk-hammaspäivystys", 
                     "Erikoissairaanhoidon konsultaatio", "Tk-konsultaatio", 
                     "Tk-apuvälinekäynti", "Asiantuntijakonsultaatio", "Tk-seulontaskopia", 
                     "Protetiikka 1 (kruunut, sillat, osa- ja kokoport.)", 
                     "Orton Oy:n potilas", "Kiireellinen käynti/psyk.", 
                     "Protetiikka 2 (rankaproteesi)", "Hoitoliukumäki", "Hoitokäynti/psyk.", 
                     "Tk-kotihoito", "Potilashotelli", "Poistunut hoidotta", 
                     "Avohoitokonsultaatio", "Tutkimus- ja toimenpidekäynti", 
                     "Tk-sarjahoito", "Projektikäynti, ilman henkilök. (ent PROTI-V)", 
                     "Potilasryhmäkäynti", "Ryhmävastaanotto", "Päätöskäynti/psyk.", 
                     "Perhepesäkäynti", "Projektikäynti, il henk&til (ent. PROPE-W)")

    kaynti_sample <- readRDS("./results/data/apotti_kaynti.rds") %>%
        filter(henkilotunnus == ID) %>%
        filter(kayntityyppi_selite %in% real_kaynti) %>%
        mutate(start = alkuhetki, end = loppuhetki, label = "kaynti")

    oh_sample <- readRDS("./results/data/apotti_osastohoito.rds") %>%
        filter(henkilotunnus == ID) %>%
        mutate(start = ohjakso_alkuhetki, end = ohjakso_loppuhetki, label = "oh")

    diag_sample <- readRDS("./results/data/apotti_diagnoosi.rds") %>%
        filter(henkilotunnus == ID)
    contacts_coldrop <- diag_sample[, c(1, 10:18)]
    contacts_sample <- contacts_coldrop %>%
        group_by(palvelutuote_numero) %>%
        mutate(num_diags = max(palvelutuote_dg_jarjestys_nro)) %>%
        slice(1) %>%
        ungroup() %>%
        mutate(start = kontakti_alkuhetki, end = kontakti_loppuhetki, label = "diag")

    proc_sample <- readRDS("./results/data/apotti_toimenpide.rds") %>%
        filter(henkilotunnus == ID) %>%
        mutate(start = toimenpide_hetki, end = toimenpide_hetki, label = "toim") %>%
        rename(kayntityyppi_koodi_proc = kayntityyppi_koodi)

    oper_sample <- readRDS("./results/data/apotti_operaatiot.rds") %>%
        filter(henkilotunnus == ID) %>%
        mutate(start = if_else(!is.na(potilas_saliin) | !is.na(potilas_salista),  if_else(!is.na(potilas_saliin), potilas_saliin, potilas_salista), operaatio_pvm),
               end = if_else(!is.na(potilas_saliin) | !is.na(potilas_salista), if_else(!is.na(potilas_salista), potilas_salista, potilas_saliin), operaatio_pvm)) %>%
        mutate(label = "oper") %>%
        rename(kiireellisyys_oper = kiireellisyys,
               asa_luokka_oper = asa_luokka,
               pot_eala_koodi_oper = pot_eala_koodi)

    transf_sample <- readRDS("./results/data/transfusions_clean_2.rds") %>%
        filter(henkilotunnus == ID) %>%
        mutate(start = veri_alku_aika, end = veri_loppu_aika, label = "transf")

    master <- bind_rows(contacts_sample,
                        kaynti_sample,
                        oh_sample,
                        proc_sample,
                        oper_sample,
                        transf_sample)

    master$label <- as.factor(master$label)
    master <- master %>%
        mutate(ymin = case_when(label == "diag" ~ -0.5,
                            label == "kaynti" ~ 0.25,
                            label == "oh" ~ -0.75,
                            label == "toim" ~ -1.25,
                            label == "oper" ~ -1.75,
                            label == "transf" ~ -2),
           ymax = case_when(label == "diag" ~ 0.25,
                            label == "kaynti" ~ 1,
                            label == "oh" ~ 0,
                            label == "toim" ~ -0.5,
                            label == "oper" ~ -1,
                            label == "transf" ~ 2))

    return(master)
}

addSmallLegend <- function(myPlot, pointSize = 0.5, textSize = 3, spaceLegend = 0.1) {
    myPlot +
        guides(shape = guide_legend(override.aes = list(size = pointSize)),
               color = guide_legend(override.aes = list(size = pointSize))) +
        theme(legend.title = element_text(size = textSize), 
              legend.text  = element_text(size = textSize),
              legend.key.size = unit(spaceLegend, "lines"))
}

splitAtCentralWhitespace <- function(strings, maxLength) {
  # Function to split a single string at its "central" whitespace
  splitString <- function(s) {
    if (nchar(s) <= maxLength) {
      return(s)
    } else {
      midPoint <- nchar(s) / 2
      whitespaces <- gregexpr("\\s", s)[[1]]  # Find all whitespaces
      # Find the whitespace closest to the middle of the string
      centralWhitespace <- whitespaces[which.min(abs(whitespaces - midPoint))]
      # Insert a linebreak at the central whitespace
      return(substring(s, 1, centralWhitespace - 1) %s+% "\n" %s+% substring(s, centralWhitespace + 1))
    }
  }
  
  # Ensure that the stringi package is available for the %s+% operator
  if (!requireNamespace("stringi", quietly = TRUE)) {
    install.packages("stringi")
  }
  library(stringi)
  
  # Apply the split function to each string in the vector
  sapply(strings, splitString)
}

#####
# Blood volume calculators
#####

# Nadler
nadler <- function(height, weight, sex) {

  if (sex %in% c("Mies", "Male", "Man")) {
    bv = (0.3669 * height^3) + (0.03219 * weight) + 0.6041
    return(bv)
  }

  if (sex %in% c("Nainen", "Female", "Woman")) {
    bv = (0.3561 * height^3) + (0.03308 * weight) + 0.1833
    return(bv)
  }
}
# Lemmens-Bernstein-Brodsky
lemmensbb <- function(weight, bmi, sex) {
  bv = weight * 70 / sqrt(bmi / 22)
  return(bv)
}

####
# Bootstrapping
####

# Bootstrap medians
md_f <- function(data, i) {
    sampled <- data[i]
    return(median(sampled))
}

####
# BMI data
####

# Functions to calculate the height/weight/bmi
calc_bmi <- function(h, w){
    if(length(w) > 1 | length(h) > 1){ # If input is a vector
        if(h[1] > 10){
            h <- h/100
        }
        return(round(w/(h)^2, 1))
    }
    if(h[1] > 10){
        h <- h/100
    }
    if(h < 1 | h > 3 | is.na(h) | is.na(w) | w < 10 | w > 500){
        return(NA)
    }
    round(w/(h)^2, 1)
}
calc_w <- function(h, bmi){
    if(length(bmi) > 1 | length(h) > 1){ # If input is a vector
        if(h[1] > 10){
            h <- h/100
        }
        return(round(bmi*(h)^2, 1))
    }
    if(h > 10){
        h <- h/100
    }
    if(is.na(h) | is.na(bmi) | h < 1 | h > 3 | bmi < 5 | bmi > 100) {
        return(NA)
    }
    round(bmi*(h)^2, 1)
}
calc_h <- function(w, bmi){
    if(length(bmi) > 1 | length(w) > 1){ # If input is a vector
        return(round(sqrt(w/bmi)*100))
    }
    if(is.na(bmi) | is.na(w) | bmi < 5 | bmi > 100 | w < 10 | w > 500) {
        return(NA)
    }
    round(sqrt(w/bmi)*100)
}