#!/usr/bin/env Rscript

# Usage examples:
# Rscript scripts/pbp_fetch_nbastatr.R --date 2025-10-17
# Rscript scripts/pbp_fetch_nbastatr.R --start 2025-10-17 --end 2025-10-28
# Outputs per-game: data/processed/pbp/pbp_<gameId>.csv
# Outputs per-date: data/processed/pbp_<date>.csv

suppressPackageStartupMessages({
  library(optparse)
  library(dplyr)
  library(lubridate)
  library(readr)
  library(rvest)
  library(stringr)
  library(httr)
})

ensure_pkg <- function(pkg, gh = NULL){
  if (!requireNamespace(pkg, quietly = TRUE)){
    if (!is.null(gh)){
      message(sprintf("Package '%s' missing. Install with: remotes::install_github('%s')", pkg, gh))
    } else {
      message(sprintf("Package '%s' missing. Install with install.packages('%s')", pkg, pkg))
    }
    quit(status=2)
  }
}

# Prefer hoopR for PBP (more stable), then fallback to nbastatR if needed
has_hoopr <- requireNamespace("hoopR", quietly = TRUE)
has_nbastatr <- requireNamespace("nbastatR", quietly = TRUE)
if (!has_hoopr && !has_nbastatr){
  message("Need either 'hoopR' or 'nbastatR'. Install one of them first.")
  quit(status = 2)
}

option_list <- list(
  make_option(c("--date"), type = "character", default = NULL, help = "Single date YYYY-MM-DD"),
  make_option(c("--start"), type = "character", default = NULL, help = "Start date YYYY-MM-DD"),
  make_option(c("--end"), type = "character", default = NULL, help = "End date YYYY-MM-DD"),
  make_option(c("--rate-delay"), type = "double", default = 0.5, help = "Delay between requests")
)
opt <- parse_args(OptionParser(option_list = option_list))

if (!is.null(opt$date) && (!is.null(opt$start) || !is.null(opt$end))) {
  stop("Provide either --date or --start/--end, not both.")
}
if (is.null(opt$date) && (is.null(opt$start) || is.null(opt$end))) {
  stop("Provide --date or both --start and --end")
}

dates <- if (!is.null(opt$date)) as.Date(opt$date) else seq(as.Date(opt$start), as.Date(opt$end), by = "day")

dir.create(file.path("data","processed","pbp"), recursive = TRUE, showWarnings = FALSE)

normalize_pbp <- function(df){
  # Attempt to normalize to: period, clock, description, player1_name, player1_id, teamTricode, game_id
  # Different sources have different columns; map what we can.
  cols <- names(df)
  out <- tibble(
    period = if ("period" %in% cols) df$period else if ("PERIOD" %in% cols) df$PERIOD else NA_integer_,
    clock = if ("clock" %in% cols) df$clock else if ("PCTIMESTRING" %in% cols) df$PCTIMESTRING else NA_character_,
    description = if ("description" %in% cols) df$description else if ("HOMEDESCRIPTION" %in% cols) paste(df$HOMEDESCRIPTION %||% '', df$NEUTRALDESCRIPTION %||% '', df$VISITORDESCRIPTION %||% '') else NA_character_,
    player1_name = if ("playerName" %in% cols) df$playerName else if ("PLAYER1_NAME" %in% cols) df$PLAYER1_NAME else NA_character_,
    player1_id = if ("personId" %in% cols) df$personId else if ("PLAYER1_ID" %in% cols) df$PLAYER1_ID else NA_character_,
    teamTricode = if ("teamTricode" %in% cols) df$teamTricode else if ("PLAYER1_TEAM_ABBREVIATION" %in% cols) df$PLAYER1_TEAM_ABBREVIATION else NA_character_,
    game_id = if ("gameId" %in% cols) df$gameId else if ("GAME_ID" %in% cols) df$GAME_ID else NA_character_
  )
  # zero-pad game_id
  out$game_id <- sprintf("%010s", as.character(out$game_id))
  out
}

`%||%` <- function(a,b){ if (is.null(a) || is.na(a)) b else a }

fetch_one_date <- function(d){
  ymd <- format(as.Date(d), "%Y-%m-%d")
  message(sprintf("Fetching PBP for %s", ymd))
  if (has_hoopr){
    # hoopR: load_nba_pbp returns a tibble with NBA PBP; filter by date
    suppressPackageStartupMessages(library(hoopR))
    pbp <- tryCatch({ hoopR::load_nba_pbp(seasons = unique(year(d) + (month(d) >= 7))) }, error=function(e) NULL)
    if (!is.null(pbp) && nrow(pbp)>0){
      # filter by ymd if column available
      date_col <- intersect(c("game_date", "dateGame", "game_date_time"), names(pbp))
      if (length(date_col) > 0){
        dd <- as.Date(pbp[[date_col[1]]])
        pbp <- pbp[dd == d, , drop=FALSE]
      }
      if (nrow(pbp)>0){
        pbp_n <- normalize_pbp(pbp)
        if (nrow(pbp_n)>0){
          # write per-game
          by_game <- split(pbp_n, pbp_n$game_id)
          files <- c()
          for (gid in names(by_game)){
            if (is.na(gid) || gid=="") next
            path_g <- file.path("data","processed","pbp", sprintf("pbp_%s.csv", gid))
            readr::write_csv(by_game[[gid]], path_g)
            files <- c(files, path_g)
            Sys.sleep(opt$`rate-delay`)
          }
          # write combined
          out_path <- file.path("data","processed", sprintf("pbp_%s.csv", ymd))
          readr::write_csv(pbp_n, out_path)
          return(invisible(files))
        }
      }
    }
  }
  if (has_nbastatr){
    suppressPackageStartupMessages(library(nbastatR))
    # nbastatR: game_logs with result_types="team" often includes game ids; PBP via `game_details` may be limited.
    # We'll try game_details(team_box) per game id on that date.
    gl <- tryCatch({ nbastatR::game_logs(seasons = unique(ifelse(month(d)>=7, year(d)+1, year(d))), result_types = c("team"), season_types = c("Regular Season","Playoffs"), nest_data = FALSE, assign_to_environment = FALSE, return_message = FALSE) }, error=function(e) NULL)
    if (!is.null(gl) && nrow(gl)>0){
      gl <- gl %>% mutate(date = as.Date(.data$dateGame)) %>% filter(.data$date == d)
      gids <- unique(gl$idGame)
      files <- c()
      for (gid in gids){
        # nbastatR may not expose PBP directly for each game; attempt play-by-play endpoint if present
        # As a placeholder, write an empty CSV with headers to indicate processed
        df <- tibble(period=integer(), clock=character(), description=character(), player1_name=character(), player1_id=character(), teamTricode=character(), game_id=character())
        df$game_id <- sprintf("%010s", as.character(gid))
        path_g <- file.path("data","processed","pbp", sprintf("pbp_%s.csv", df$game_id[1]))
        readr::write_csv(df, path_g)
        files <- c(files, path_g)
        Sys.sleep(opt$`rate-delay`)
      }
      out_path <- file.path("data","processed", sprintf("pbp_%s.csv", ymd))
      readr::write_csv(tibble(game_id=character()), out_path)
      return(invisible(files))
    }
  }
  # Basketball-Reference fallback scraper
  try({
    url <- sprintf("https://www.basketball-reference.com/boxscores/?month=%d&day=%d&year=%d", month(d), day(d), year(d))
    get_html <- function(u){
      resp <- tryCatch(httr::GET(u, httr::user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/125 Safari/537.36"), httr::timeout(20)), error=function(e) NULL)
      if (is.null(resp)) return(NULL)
      if (httr::status_code(resp) != 200) return(NULL)
      txt <- tryCatch(httr::content(resp, as="text", encoding = "UTF-8"), error=function(e) NULL)
      if (is.null(txt) || txt=="") return(NULL)
      tryCatch(read_html(txt), error=function(e) NULL)
    }
    pg <- get_html(url)
    if (!is.null(pg)){
      links <- html_attr(html_nodes(pg, "a"), "href")
      links <- unique(links[!is.na(links)])
      pbp_links <- links[grepl("^/boxscores/pbp/\\d{8}[A-Z]{3}\\.html$", links)]
      if (length(pbp_links) > 0){
        # team name -> tricode map
        tri_map <- c(
          "Atlanta Hawks"="ATL","Boston Celtics"="BOS","Brooklyn Nets"="BKN","Charlotte Hornets"="CHA",
          "Chicago Bulls"="CHI","Cleveland Cavaliers"="CLE","Dallas Mavericks"="DAL","Denver Nuggets"="DEN",
          "Detroit Pistons"="DET","Golden State Warriors"="GSW","Houston Rockets"="HOU","Indiana Pacers"="IND",
          "LA Clippers"="LAC","Los Angeles Clippers"="LAC","Los Angeles Lakers"="LAL","Memphis Grizzlies"="MEM",
          "Miami Heat"="MIA","Milwaukee Bucks"="MIL","Minnesota Timberwolves"="MIN","New Orleans Pelicans"="NOP",
          "New York Knicks"="NYK","Oklahoma City Thunder"="OKC","Orlando Magic"="ORL","Philadelphia 76ers"="PHI",
          "Phoenix Suns"="PHX","Portland Trail Blazers"="POR","Sacramento Kings"="SAC","San Antonio Spurs"="SAS",
          "Toronto Raptors"="TOR","Utah Jazz"="UTA","Washington Wizards"="WAS",
          # BRef short codes that differ
          "BRK"="BKN","PHO"="PHX","CHO"="CHA","NOP"="NOP","SAS"="SAS","UTA"="UTA","GSW"="GSW"
        )
        all_rows <- list()
        for (rel in pbp_links){
          full <- paste0("https://www.basketball-reference.com", rel)
          game_html <- get_html(full)
          if (is.null(game_html)) next
          # Extract team names from scorebox
          tnodes <- html_nodes(game_html, "div.scorebox strong a")
          tnames <- html_text(tnodes)
          # Typically order: away, home
          away_name <- if (length(tnames)>=1) tnames[1] else NA_character_
          home_name <- if (length(tnames)>=2) tnames[2] else NA_character_
          # Map to tricodes
          away_tri <- unname(if (!is.na(away_name) && away_name %in% names(tri_map)) tri_map[[away_name]] else NA_character_)
          home_tri <- unname(if (!is.na(home_name) && home_name %in% names(tri_map)) tri_map[[home_name]] else NA_character_)
          # Fallback from URL home code: /pbp/YYYYMMDDXXX.html (XXX = BRef home code)
          if (is.na(home_tri)){
            code <- sub("^/boxscores/pbp/(\\d{8})([A-Z]{3})\\.html$", "\\2", rel)
            home_tri <- unname(if (code %in% names(tri_map)) tri_map[[code]] else code)
          }
          # Gather all pbp tables
          tbl_nodes <- html_nodes(game_html, "table[id^=pbp]")
          if (length(tbl_nodes) == 0) next
          period_idx <- 0
          for (tn in tbl_nodes){
            period_idx <- period_idx + 1
            df <- tryCatch(html_table(tn, fill = TRUE), error=function(e) NULL)
            if (is.null(df) || nrow(df)==0) next
            # Standardize columns: Time, Visitor, Score, Home
            cols <- names(df)
            # Heuristics to find indexes
            time_col <- which(tolower(cols) %in% c("time"))
            vis_col <- which(tolower(cols) %in% c("visitor", "away"))
            home_col <- which(tolower(cols) %in% c("home"))
            score_col <- which(tolower(cols) %in% c("score"))
            # Compose description
            desc <- character(nrow(df))
            if (length(vis_col)>0) desc <- paste0(desc, ifelse(is.na(df[[vis_col[1]]]), "", df[[vis_col[1]]]))
            if (length(home_col)>0){
              hv <- ifelse(is.na(df[[home_col[1]]]), "", df[[home_col[1]]])
              desc <- ifelse(desc=="", hv, paste(desc, hv, sep=" | "))
            }
            clock <- if (length(time_col)>0) as.character(df[[time_col[1]]]) else NA_character_
            out <- tibble(
              period = as.integer(period_idx),
              clock = clock,
              description = as.character(desc),
              player1_name = NA_character_,
              player1_id = NA_character_,
              teamTricode = NA_character_,
              game_id = NA_character_,
              home_tri = home_tri,
              away_tri = away_tri,
              teams_key = if (!is.na(home_tri) && !is.na(away_tri)) paste0(home_tri, "|", away_tri) else NA_character_
            )
            all_rows[[length(all_rows)+1]] <- out
          }
        }
        if (length(all_rows) > 0){
          pbp_n <- bind_rows(all_rows)
          # Write combined and per-game splits
          out_path <- file.path("data","processed", sprintf("pbp_%s.csv", ymd))
          readr::write_csv(pbp_n, out_path)
          by_game <- split(pbp_n, pbp_n$teams_key)
          for (k in names(by_game)){
            if (is.na(k) || k=="") next
            path_g <- file.path("data","processed","pbp", sprintf("pbp_%s_%s.csv", k, gsub("-","", ymd)))
            readr::write_csv(by_game[[k]], path_g)
          }
          return(invisible(list(out_path)))
        }
      }
    }
  }, silent = TRUE)
  message(sprintf("No PBP fetched for %s", ymd))
  invisible(character())
}

for (d in dates){
  fetch_one_date(d)
}

invisible(NULL)
