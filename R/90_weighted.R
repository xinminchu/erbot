########################################

#' Normalize named field weights
#' @export
normalize_weights <- function(weights, field_names) {
  w <- unlist(weights)
  if (is.null(names(w))) {
    if (length(w) != length(field_names)) stop("weights length must match fields")
    names(w) <- field_names
  } else {
    w <- w[field_names]; w[is.na(w)] <- 0
  }
  if (any(w < 0)) stop("weights must be non-negative")
  s <- sum(w); if (s <= 0) stop("sum(weights) must be > 0")
  w / s
}

#' Expand fields proportionally to weights (fallback)
#' @export
expand_fields_by_weights <- function(fields, weights, base_rep = 10L) {
  w <- normalize_weights(weights, fields)
  reps <- pmax(1L, round(w * base_rep))
  expanded <- unlist(mapply(function(f, r) rep(f, r), fields, reps, SIMPLIFY = FALSE), use.names = FALSE)
  attr(expanded, "weights") <- w
  attr(expanded, "reps") <- reps
  expanded
}


########################################
