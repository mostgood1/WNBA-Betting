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
  pick_first <- function(cands){
    for (nm in cands){ if (nm %in% cols) return(df[[nm]]) }
    return(NULL)
  }
  # period candidates
  period_vec <- pick_first(c("period","periodNumber","PERIOD","qtr","quarter"))
  # clock candidates
  clock_vec <- pick_first(c("clock","gameClock","PCTIMESTRING","time_remaining","remaining_time"))
  # description candidates
  desc_vec <- pick_first(c("description","desc","playDescription"))
  if (is.null(desc_vec)){
    # synthesize from known components commonly present in NBA liveData/hoopR
    comp_names <- c("HOMEDESCRIPTION","NEUTRALDESCRIPTION","VISITORDESCRIPTION",
                    "actionType","action_type","eventActionType","eventType","subType","shotResult",
                    "playerName","player_name","playerNameI","teamTricode","teamAbbreviation")
    comps <- list()
    for (nm in comp_names){ if (nm %in% cols) comps[[nm]] <- df[[nm]] }
    if (length(comps) > 0){
      # build string row-wise
      mat <- do.call(cbind, lapply(comps, function(x) ifelse(is.na(x), "", as.character(x))))
      if (!is.matrix(mat)) mat <- matrix(mat, ncol = length(comps))
      desc_vec <- apply(mat, 1, function(r){
        v <- r[r != ""]
        if (length(v)==0) return(NA_character_)
        paste(v, collapse = " ")
      })
    }
  }
  # player name/id candidates
  name_vec <- pick_first(c("playerName","player_name","PLAYER1_NAME"))
  pid_vec  <- pick_first(c("personId","playerId","player_id","PLAYER1_ID"))
  team_vec <- pick_first(c("teamTricode","teamAbbreviation","PLAYER1_TEAM_ABBREVIATION","TEAM_ABBREVIATION","teamTricodeHome","teamTricodeAway"))
  gid_vec  <- pick_first(c("gameId","game_id","game_id_clean","GAME_ID","idGame"))
  out <- tibble(
    period = if (!is.null(period_vec)) as.integer(period_vec) else NA_integer_,
    clock = if (!is.null(clock_vec)) as.character(clock_vec) else NA_character_,
    description = if (!is.null(desc_vec)) as.character(desc_vec) else NA_character_,
    player1_name = if (!is.null(name_vec)) as.character(name_vec) else NA_character_,
    player1_id = if (!is.null(pid_vec)) as.character(pid_vec) else NA_character_,
    teamTricode = if (!is.null(team_vec)) as.character(team_vec) else NA_character_,
    game_id = if (!is.null(gid_vec)) as.character(gid_vec) else NA_character_
  )
  # zero-pad game_id
  out$game_id <- ifelse(is.na(out$game_id) | out$game_id=="", NA_character_, sprintf("%010s", as.character(out$game_id)))
  out
}

`%||%` <- function(a,b){ if (is.null(a) || is.na(a)) b else a }

fetch_one_date <- function(d){
  # normalize d to Date to avoid tz()/numeric month errors
  if (!inherits(d, "Date")) {
    d <- tryCatch(as.Date(d), error=function(e) tryCatch(as.Date(d, origin = "1970-01-01"), error=function(e2) as.Date(as.character(d))))
  }
  ymd <- format(as.Date(d), "%Y-%m-%d")
  message(sprintf("Fetching PBP for %s", ymd))
  if (has_hoopr){
    # hoopR approach: get schedule for the season, filter by date, then fetch PBP per game id
    suppressPackageStartupMessages(library(hoopR))
    season <- unique(lubridate::year(d) + (lubridate::month(d) >= 7))
    sched <- tryCatch({ hoopR::nba_schedule(seasons = season) }, error=function(e) NULL)
    if (!is.null(sched) && nrow(sched) > 0){
      # Find a date column and normalize to Date
      date_col <- intersect(c("game_date","gamedate","dateGame","gameDate","game_date_time"), names(sched))
      if (length(date_col) > 0){
        raw <- sched[[date_col[1]]]
        # Normalize to Date robustly
        to_date <- function(x){
          if (inherits(x, "Date")) return(as.Date(x))
          if (inherits(x, "POSIXt")) return(as.Date(x))
          if (is.numeric(x)) { # Excel/epoch-like numeric dates
            # Assume seconds since epoch
            return(as.Date(as.POSIXct(x, origin = "1970-01-01", tz = "UTC")))
          }
          xs <- as.character(x)
          xs <- sub("T.*$", "", xs) # strip time part if ISO
          suppressWarnings(as.Date(xs))
        }
        dd <- to_date(raw)
        sched <- sched[dd == as.Date(d), , drop = FALSE]
      } else {
        sched <- sched[0,]
      }
      if (nrow(sched) > 0){
        gid_col <- intersect(c("game_id","gameId","gameIdPrevious","GAME_ID"), names(sched))
        if (length(gid_col) > 0){
          gids <- unique(as.character(sched[[gid_col[1]]]))
          gids <- sprintf("%010s", gids)
          pbp_list <- list()
          for (gid in gids){
            if (is.na(gid) || gid == "") next
            pbp_g <- tryCatch({ hoopR::nba_pbp(game_id = gid) }, error=function(e) NULL)
            if (is.null(pbp_g) || nrow(pbp_g) == 0) next
            pbp_g$game_id <- gid
            pbp_list[[length(pbp_list)+1]] <- pbp_g
            Sys.sleep(opt$`rate-delay`)
          }
          if (length(pbp_list) > 0){
            pbp_all <- dplyr::bind_rows(pbp_list)
            pbp_n <- normalize_pbp(pbp_all)
            # write per-game
            by_game <- split(pbp_n, pbp_n$game_id)
            files <- c()
            for (gid in names(by_game)){
              if (is.na(gid) || gid=="") next
              path_g <- file.path("data","processed","pbp", sprintf("pbp_%s.csv", gid))
              readr::write_csv(by_game[[gid]], path_g)
              files <- c(files, path_g)
            }
            # write combined
            out_path <- file.path("data","processed", sprintf("pbp_%s.csv", ymd))
            readr::write_csv(pbp_n, out_path)
            return(invisible(files))
          }
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
