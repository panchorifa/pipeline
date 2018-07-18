# Doximity's Python Software Engineer, Data Integration Assignment
##### Proprietary and Confidential
------------

The central purpose of the Data Integration team at Doximity is to get data from outside sources into a format that can be used in production applications within the company. Examples of that data might be things like the [NPPES NPI dataset](http://download.cms.gov/nppes/NPI_Files.html), [Physician Compare dataset](https://data.medicare.gov/data/physician-compare), or [state license data](https://appsmqa.doh.state.fl.us/downloadnet/licensure.aspx).

We've put together a small sample from some NPI data dumps (the full ones are >1gb each).

```
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-02-07.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-03-13.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-04-10.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-05-08.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-06-12.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-07-10.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-08-07.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-09-11.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-10-09.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-11-13.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-12-11.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-01-08.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-02-12.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-03-12.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-04-09.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-05-07.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-06-11.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-07-09.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-08-13.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-09-10.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-10-08.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-11-12.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2017-12-10.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2018-01-07.csv
https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2018-02-13.csv
```

Your task is to create a re-usable pipeline that can be used to copy data from a HTTP URL into a MySQL database. This should be a _generic_ pipeline that could be re-used for multiple datasets, not just this NPI CSV.

We will invoke your pipeline with e.g.:

```
./pipeline.py --source https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-02-07.csv --sink mysql://root:password@mysql/external/npi
./pipeline.py --source https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2016-03-13.csv --sink mysql://root:password@mysql/external/npi
# ...
./pipeline.py --source https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2018-01-07.csv --sink mysql://root:password@mysql/external/npi
./pipeline.py --source https://s3.amazonaws.com/dox-hiring-samples/npi-samples/npi-sample-2018-02-13.csv --sink mysql://root:password@mysql/external/npi
```

At the end, we expect the data to be in an `npi` table in the `external` database in the `mysql` host/container authenticated with `root`/`password`.

However, ideally we'd also be able to run it with `./pipeline.py --source https://www.example.org/some/other/dataset.csv --sink mysql://ben:hunter2@mysql/external/other_dataset` and import into `external.other_dataset` without many changes. That kind of re-usable code is what we're trying to create in the Data Integration team.

We're looking for well architected, object-oriented, and testable code.

Think about the way you design things like primary keys, tracking change timestamps, and auditing a piece of data.

Some questions that come up during day-to-day work at Doximity are:

* What is this user's current address?
* When did this user change their name? What was their previous name?
* When was this NPI number removed from the active NPI list? When was it re-activated?
* Why do we have the wrong address listed for this physician? What dump did this data point come from?
* When did this user move to a different city?

Try to capture enough data in your result tables that we can answer those questions.

In the sample dataset, there was a schema change in 2017-01-01. It's up to you how you want to handle that.


------------

Please submit all relevant code and write a detailed explanation of your methodology and results. Documenting your thought process will help us evaluate your submission.

We’re a Python shop - feel free to use any libraries you see fit. Note that in reviewing your challenge, we'll be assessing the pipeline you devise, code quality, and clarity of thought in working through the problem.

------------

## Submission instructions

**Please follow all listed steps to ensure a prompt review of your submission by our data and software engineering hiring team:**
1. Provide your Doximity point of contact with your GitLab username (create a new account if you do not already have one). Your point of contact will add you to the data challenge repository. Once you've been added to the repo, you should see in the Project tab the `python-engineer-data` project containing the challenge instructions.
2. Fork the `python-engineer-data` repository.
3. In the `python-engineer-data` project tab, click on `Members`. Under `Project members`, click on the `Add member` tab. Under `Select members to invite`, please type in `Doximity-data` and choose the `Developer` role permission. When done, click `Add to project`. *This will give us permissions to review your challenge submission when complete.*
4. In the forked `python-engineer-data` project, create a new branch `lastname-firstname`. Work on the assignment and commit your changes to the `lastname-firstname` branch.
5. When complete with the assignment, after having commit all your changes, create a new `Merge Request`. Add `Doximity-data` as an assignee. *This will trigger an email notification to our team to review your submission.*

* For a visual walk-through of the submission process, please watch [this video](https://vimeo.com/227828054/562c3f6acf). 
* If any questions come up, please send an email to your Doximity point of contact.
