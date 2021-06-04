-- Original query from Vladimir
SELECT
       sum(is_committer) committers ,
       sum(1 - is_committer) non_committers ,
       count(*) total,
       quarter_date,
       cname
FROM
  (SELECT cast(floor(commit_timestamp TO QUARTER) AS date) quarter_date ,
          CASE
              WHEN c.author_timestamp > phonebook.first_commit THEN 1
              ELSE 0
          END is_committer,
          phonebook.committer cname
   FROM git_commits c
   LEFT JOIN
     (SELECT committer ,
             min(commit_timestamp) first_commit
      FROM git_commits
      GROUP BY committer) phonebook ON (c.author = phonebook.committer))
WHERE quarter_date = cast('2020-07-01' as date)
GROUP BY cname, quarter_date
ORDER BY non_committers DESC;


-- Show the number of non-committer (contributor) commits per month
SELECT
      EXTRACT( year FROM a.commit_timestamp) as `year`, MONTH(a.commit_timestamp) as `month`, COUNT(*) as contributor_commits
FROM git_commits a
INNER JOIN git_commits r
  ON a.`commit`=r.`commit`
WHERE a.commit_timestamp >= cast('2020-01-01' as date) AND a.commit_timestamp < cast('2021-12-30' as date)
AND NOT EXISTS (SELECT 1 FROM git_commits c WHERE a.author=c.committer)
GROUP BY EXTRACT( year FROM a.commit_timestamp), MONTH(a.commit_timestamp)
ORDER BY `year` ASC, `month` ASC;


-- Display the number of active reviewers per month
SELECT
      EXTRACT( year FROM a.commit_timestamp) as `year`, MONTH(a.commit_timestamp) as `month`, COUNT(DISTINCT a.committer) as active_reviewers
FROM git_commits a
INNER JOIN git_commits r
  ON a.`commit`=r.`commit`
WHERE a.commit_timestamp >= cast('2020-01-01' as date) AND a.commit_timestamp < cast('2021-12-30' as date)
AND NOT EXISTS (SELECT 1 FROM git_commits c WHERE a.author=c.committer)
GROUP BY EXTRACT( year FROM a.commit_timestamp), MONTH(a.commit_timestamp)
ORDER BY `year` ASC, `month` ASC;


-- Display the top reviewers in the last x months
SELECT
      a.committer, COUNT(*) as reviews
FROM git_commits a
INNER JOIN git_commits r
  ON a.`commit`=r.`commit`
WHERE a.commit_timestamp >= cast('2020-01-01' as date) AND a.commit_timestamp < cast('2021-12-30' as date)
AND NOT EXISTS (SELECT 1 FROM git_commits c WHERE a.author=c.committer)
GROUP BY a.committer
ORDER BY reviews DESC;
