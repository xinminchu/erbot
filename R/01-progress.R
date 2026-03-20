########################################
# File: D:/erbot/R/01-progress.R
########################################

#' Progress bar (start)
#' @param total_steps Integer. Total number of steps to track.
#' @param title Character. Label printed at start and finish.
#' @return A named list of class `"er_progress"` with fields `pb`, `total`,
#'   `step`, `title`, `t0`, and `last`.
#' @export
er_progress_start <- function(total_steps, title = "ER pipeline"){
  pb <- utils::txtProgressBar(min=0, max=total_steps, style=3)
  env <- list(pb=pb, total=total_steps, step=0L, title=title, t0=Sys.time(), last=Sys.time())
  class(env) <- "er_progress"
  cat(sprintf("\n[%s] %s — starting (%d steps)\n", format(env$t0, "%H:%M:%S"), title, total_steps))
  env
}

#' Progress bar (tick)
#' @param p Progress object returned by [er_progress_start()].
#' @param label Optional character label printed beside the step counter.
#' @return The updated progress object (invisibly).
#' @export
er_progress_tick <- function(p, label=NULL){
  if (!inherits(p, "er_progress")) return(invisible(NULL))
  p$step <- p$step + 1L
  utils::setTxtProgressBar(p$pb, p$step)
  now <- Sys.time()
  if (!is.null(label)) {
    cat(sprintf("\n[%s] Step %d/%d: %s (%.1fs)\n",
                format(now, "%H:%M:%S"), p$step, p$total, label,
                as.numeric(difftime(now, p$last, units="secs"))))
  }
  p$last <- now
  invisible(p)
}

#' Progress bar (done)
#' @param p Progress object returned by [er_progress_start()].
#' @return `NULL` invisibly. Closes the text progress bar and prints elapsed time.
#' @export
er_progress_done <- function(p){
  if (!inherits(p, "er_progress")) return(invisible(NULL))
  close(p$pb)
  cat(sprintf("\n[%s] %s — done. Total elapsed: %.1fs\n\n",
              format(Sys.time(), "%H:%M:%S"), p$title,
              as.numeric(difftime(Sys.time(), p$t0, units="secs"))))
  invisible(NULL)
}
