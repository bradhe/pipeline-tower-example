import os
from tower import Task
from core.readers.github import ReadGithub

# create a reader Task that will read from Github and store in memory (readitems attributes of the class)
reader = ReadGithub("GitHubReader")

# read from the source and store all elements in internal property readitems
# this is equivalent to calling reader.do(source)
print("* Reading content from GitHub")
reader("https://api.github.com/repos/dlt-hub/dlt",["issues"])

# Instantiate a task.
sk = os.environ["TOWER_SECRET_KEY"]
task = Task("bradhe/nsfw-detector", sk)

# filter based on the output from a task
print("* Finding issues that require moderation")
reader.enrich(task).write(...)
