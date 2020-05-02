# Avocado Data Processing

This repository contains code to process email data. It has been used in particular for processing the [Avocado collection](https://catalog.ldc.upenn.edu/LDC2015T03), for the following paper:

_Configuring Audiences: A Case Study of Email Communication._
Justine Zhang, James W. Pennebaker, Susan T. Dumais, Eric Horvitz
Proceedings of CSCW, 2020. [link](http://tisjune.github.io/research/audience_configurations)

As released by the Linguistic Data Consortium, the raw data presented various challenges. My code documents what these challenges are and provides methodology for addressing them. It should be runnable, **with some modification**, on other email datasets where email text is contained in .txt files, as in Avocado. I expect the challenges I encountered in using this data to also occur elsewhere and the broader purpose of releasing this code is to give others a sense of what these challenges are and how to deal with them.

## Overview of challenges and processing steps

See the paper linked above for further information about the data and processing procedure (appendix), as well as potential caveats.

There were two main challenges I sought to address, that are covered by the code here:

1. the emails surfaced by the XML entries in the original dataset are incomplete, because emails--in particular antecedents in a thread--might have been deleted from inboxes;
2. there are several duplicates, as the same email could exist in multiple inboxes of all people involved in the exchange.

I also took additional steps to clean up the text of the email, parse out prefixes like Re and Fwd in the subject line, and standardize dates.

## Overview of code

The procedure is vaguely in the style of map-reduce: to parallelize, it "maps" i.e., extracts and processes individual emails, before reducing by deduplicating them. However, the entire pipeline could probably be run completely in memory, depending on the machine.

### scripts

`settings.py` lists the names of directories where input, and intermediate and final output are stored; these directories should be created and names should be modified as appropriate.

I've listed the following files in the order in which they should be run.

`unzip_files.py`: decompresses LDC data

`extraction_utils.py`: functions to extract and parse individual emails from .txt files, separated by --- Original Message ---, as well as coalescing duplicate emails into a single entry. see the subsequent files for examples of how key functions are called.

`extract_emails.py` : extracts individual (non-deduplicated) emails.

`dedup_extracted_emails.py`: deduplicates emails, using the subject, FROM, TO, CC and timestamp as a key (somewhat conservatively). this corresponds to the first pass of deduplication as described in the paper.

`resolve_split_dup_keys.py`: very infrequently, the deduplication will put two different emails into the same key; this resolves this issue.

`group_by_thread.py` and `reshuffle_by_thread_bin.py`: groups deduplicated emails into bins (see `settings.py`) by thread.

`dedup_in_thread.py`: second deduplication pass as described in the paper. here we deduplicate on emails rooted in the same parent, keying on all but timestamp.

`proc_email_text.py`: various post-processing: dependency parses, sort of normalizes, extracts a subset of dependency arcs. note output is stored in text files, since spacy objects take up a ton of memory.

### other notes

* This code is time zone sensitive. For Avocado, it should be run in Pacific Time. More generally, we could be cleverer about timestamps (especially since they are one key we use for deduplicating emails).
* While this code should result in a more complete dataset, there are certainly caveats. Notably, if everyone involved with an email deletes it, and no reply of that email exists in the corpus, then we cannot recover that email; I expect this to disproportionately impact email threads involving people outside of the Avocado company, since we do not have access to their inboxes. In addition, since addressees in Bcc are not always listed, such information is incomplete as well.
* I have not tried to infer membership on mailing lists. The dataset contains contacts lists and address books as well, which I have not extensively explored. In the paper, I outline a procedure for associating names of Avocado employees with email addresses and, to some extent, job titles; this was done in a more ad-hoc/manual way and at a coarser granularity (in particular, more work would be needed to infer precise ranks or org chart-style information).
* This code does not cover everything -- a manual inspection of samples of emails suggests that most duplicates are resolved, but due to the diversity of email inbox formatting, there will be inevitable mistakes here and there. 