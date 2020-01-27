library(readr)
library(dplyr)
library(ggplot2)
library(scales)

function() {
	source("./plot.R")

	d <- read_csv("two_years_eval.csv") %>%
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

	accesses <- d %>% slice(1) %>% pull(accesses)
	files <- d %>% slice(1) %>% pull(files)
	total_bytes_accessed <- d %>% slice(1) %>% pull(total_bytes_accessed)
	unique_bytes_accessed <- d %>% slice(1) %>% pull(unique_bytes_accessed)
	theoretical_best_miss_rate <- d %>% slice(1) %>% pull(theoretical_best_miss_rate)
	theoretical_best_byte_miss_rate <- d %>% slice(1) %>% pull(theoretical_best_byte_miss_rate)

	format_bytes <- label_bytes(units="auto_binary", accuracy=0.1)
	format_si <- label_number_si(accuracy=0.1)

	caption_text <- paste(
		format_bytes(total_bytes_accessed),
		" accessed in ",
		format_si(accesses),
		" accesses (",
		format_bytes(unique_bytes_accessed),
		" unique bytes in ",
		format_si(files),
		" files).",
		sep=""
	)

	d %>%
		ggplot() +
			geom_point(aes(x=storage_bytes, y=miss_rate, group=processor, colour=processor)) +
			geom_line(aes(x=storage_bytes, y=miss_rate, group=processor, colour=processor)) +
			geom_hline(yintercept=theoretical_best_miss_rate) +
			scale_x_continuous(breaks=extended_breaks(Q=c(1, 1024)), labels=format_bytes) +
			labs(
				x = "Cache Volume Size",
				y = "Miss Rate",
				colour = "Algorithm",
				caption = caption_text
			)

	ggsave("two_years_mrc.pdf", width=width, height=height)

	d %>%
		ggplot() +
			geom_point(aes(x=storage_bytes, y=byte_miss_rate, group=processor, colour=processor)) +
			geom_line(aes(x=storage_bytes, y=byte_miss_rate, group=processor, colour=processor)) +
			geom_hline(yintercept=theoretical_best_byte_miss_rate) +
			scale_x_continuous(breaks=extended_breaks(Q=c(1, 1024)), labels=format_bytes) +
			labs(
				x = "Cache Volume Size",
				y = "Byte Miss Rate",
				colour = "Algorithm",
				caption = caption_text
			)

	ggsave("two_years_bmrc.pdf", width=width, height=height)
}
