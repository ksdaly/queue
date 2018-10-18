# Queue runner with NCQ

## Dependencies:
* NSQ

## Start processing queue
```
./runner.rb
```

## Stop processing queue
```
cntrl + c
```
## Post Mortem

### What went well?

The project overall went well. I spent about 30% of time planning and researching how queues are implemented in other projects. Netflix has a lot of very good information in their [tech blog](https://medium.com/netflix-techblog). I also found that game design has a lot overlap with event processing, notably, I found this [ebook](http://gameprogrammingpatterns.com/contents.html) helpful. It was also worthwile checking out tools that are design to do something similar, for example, [Apollo Client](https://www.apollographql.com/docs/react/) batches queries in time increments. The most productive moment was to describe my ideas on the back of a magazine to my husband (who is not an engineer) during breakfast. After this initial stage the implementation was pretty straight forward.

![Alt text](./readme_img.jpg?raw=true "Whiteboarding")

### What was frustrating?

I spent way too much time figuring out how ncq-client and ncq work together, and debugging issues with connection. ncq-ruby specs were vey helpful though, as I was finally able to see a working setup.

### How would I avoid this frustration?

Next time I would spend more time discovering NSQ itself to facilitate debugging in the implementation stage.

### Additional thoughts?

Keeping message data in memory could potentially result in data loss if the service would go down. Depending on how important data consistency is, the message aggregates could be persisted in a counter field in Cassandra and read from it when processing. The messages could also be writter to Hive and processed by quering data for a given timeframe. More info on that [here](https://www.infoq.com/articles/netflix-migrating-stream-processing).

