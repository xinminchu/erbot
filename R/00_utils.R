########################################

#' @importFrom magrittr %>%
NULL

#' Internal utility: Null-coalescing
#' @keywords internal
`%||%` <- function(a, b) if (!is.null(a)) a else b

#' Cosine distance on rows of a matrix
#' @param X numeric matrix
#' @return full cosine distance matrix with diag 0
#' @export
er_cosine_dist <- function(X){
  X <- as.matrix(X)
  nr <- sqrt(rowSums(X^2)); nr[nr == 0] <- 1
  X <- X / nr
  D <- 1 - (X %*% t(X))
  pmax(D, 0)
}

# ---- Progress helpers ----------------------------------------------------

#' Progress bar (start)
#' @param total_steps integer
#' @param title character
#' @return progress environment
#' @export
er_progress_start <- function(total_steps, title = "ER pipeline"){
  pb <- utils::txtProgressBar(min=0, max=total_steps, style=3)
  env <- list(pb=pb, total=total_steps, step=0L, title=title, t0=Sys.time(), last=Sys.time())
  class(env) <- "er_progress"
  cat(sprintf("\n[%s] %s — starting (%d steps)\n", format(env$t0, "%H:%M:%S"), title, total_steps))
  env
}

#' Progress bar (tick)
#' @param p progress object from [er_progress_start]
#' @param label optional label
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
#' @param p progress object
#' @export
er_progress_done <- function(p){
  if (!inherits(p, "er_progress")) return(invisible(NULL))
  close(p$pb)
  cat(sprintf("\n[%s] %s — done. Total elapsed: %.1fs\n\n",
              format(Sys.time(), "%H:%M:%S"), p$title,
              as.numeric(difftime(Sys.time(), p$t0, units="secs"))))
  invisible(NULL)
}

# ---- ID alignment + safe vertex attribute helpers -----------------------

#' Ensure a vector is named by global IDs
#' @keywords internal
ensure_named_by_ids <- function(x, ids, x_name = "vector") {
  if (is.null(names(x))) {
    stop(sprintf("'%s' must be a named vector with names = global IDs.", x_name))
  }
  if (!is.character(names(x))) {
    stop(sprintf("names(%s) must be character IDs.", x_name))
  }
  if (anyDuplicated(names(x))) {
    dup <- unique(names(x)[duplicated(names(x))])
    stop(sprintf("Duplicate IDs in names(%s): e.g., %s", x_name, paste(head(dup, 5), collapse = ", ")))
  }
  invisible(TRUE)
}

#' Map a named full-length vector (names = global IDs) to vertices of g
#' @keywords internal
map_vals_to_graph <- function(g, vals) {
  ensure_named_by_ids(vals, ids = names(vals), x_name = "vals")
  ids <- igraph::as_ids(igraph::V(g))
  vals[ids]  # may introduce NA where ids are missing
}

#' Safe setter that auto-aligns named vectors to vertices in 'index'
#' @export
safe_set_vertex_attr <- function(g, name, values, index = igraph::V(g)) {
  # length 1 is always okay
  if (length(values) == 1L) {
    return(igraph::set_vertex_attr(g, name, index = index, value = values))
  }
  # align by names if possible
  if (!is.null(names(values))) {
    idx_names <- igraph::as_ids(index)
    aligned   <- values[idx_names]
    return(igraph::set_vertex_attr(g, name, index = index, value = aligned))
  }
  # otherwise lengths must match
  if (length(values) != length(index)) {
    stop(sprintf("safe_set_vertex_attr: length(values)=%d != length(index)=%d and values are not named.",
                 length(values), length(index)))
  }
  igraph::set_vertex_attr(g, name, index = index, value = values)
}


########################################
