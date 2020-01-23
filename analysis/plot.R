library(readr)
library(dplyr)
library(ggplot2)
library(gdata)

function() {
	d <- read_csv("two_months_eval.csv") %>%
		mutate(
			theoretical_best_hit_rate = (accesses - files) / accesses,
			theoretical_best_miss_rate = files / accesses,
			theoretical_best_byte_hit_rate = (total_bytes_accessed - unique_bytes_accessed) / total_bytes_accessed,
			theoretical_best_byte_miss_rate = unique_bytes_accessed / total_bytes_accessed,
			hit_rate = files_hit / accesses,
			miss_rate = files_missed / accesses,
			byte_hit_rate = bytes_hit / total_bytes_accessed,
			byte_miss_rate = bytes_missed / total_bytes_accessed,
			storage_bytes = storage_size * 1024 * 1024 * 1024
		)

	width <- 8
	height <- 5

	total_bytes_accessed <- d %>% slice(1) %>% pull(total_bytes_accessed)
	unique_bytes_accessed <- d %>% slice(1) %>% pull(unique_bytes_accessed)
	theoretical_best_miss_rate <- d %>% slice(1) %>% pull(theoretical_best_miss_rate)
	theoretical_best_byte_miss_rate <- d %>% slice(1) %>% pull(theoretical_best_byte_miss_rate)

	caption_text <- paste(humanReadable(total_bytes_accessed), " accessed (", humanReadable(unique_bytes_accessed), " unique bytes). ", sep="")

	library(scales)

	d %>%
		ggplot() +
			geom_point(aes(x=storage_bytes, y=miss_rate, group=processor, colour=processor)) +
			geom_line(aes(x=storage_bytes, y=miss_rate, group=processor, colour=processor)) +
			geom_hline(yintercept=theoretical_best_miss_rate) +
			scale_x_continuous(breaks=extended_breaks(Q=c(1, 1024)), labels=humanReadableLabs()) +
			labs(
				x = "Cache Volume Size [Giga Byte]",
				y = "Miss Rate",
				colour = "Algorithm",
				caption = caption_text
			)

	ggsave("two_months_mrc.pdf", width=width, height=height)

	d %>%
		ggplot() +
			geom_point(aes(x=storage_size, y=byte_miss_rate, group=processor, colour=processor)) +
			geom_line(aes(x=storage_size, y=byte_miss_rate, group=processor, colour=processor)) +
			geom_hline(yintercept=theoretical_best_byte_miss_rate) +
			labs(
				x = "Cache Volume Size [Giga Byte]",
				y = "Byte Miss Rate",
				colour = "Algorithm",
				caption = caption_text
			)

	ggsave("two_months_bmrc.pdf", width=width, height=height)
}

humanReadableLabs <- function(units="auto", standard="IEC", digits=1, width=NULL, sep=" ", justify="right") {
	function(x) {
		sapply(x, function(val) {
			if (is.na(val)) {
				return("")
			} else {
				return(
					humanReadable(val, units=units, standard=standard, digits=digits, width=width, sep=sep, justify=justify)
				)
			}
		})
	}
}
