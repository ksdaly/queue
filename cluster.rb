require 'nsq-cluster'

class Cluster
  attr_accessor :cluster

  def initialize
    @cluster = NsqCluster.new(nsqd_count: 2, nsqlookupd_count: 2, nsqd_options: { verbose: true }, nsqlookupd_options: { verbose: true })
    nsqd = cluster.nsqd.first
    nsqd.create(topic: 'videos')
    nsqd.create(topic: 'videos', channel: 'default')

    sleep(3)
  end

  def destroy
    cluster.destroy
  end
end
