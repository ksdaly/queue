#!/usr/bin/env ruby

require_relative './video'
require_relative './message'
require_relative './cluster'
require_relative './producer'
require_relative './consumer'

begin
  puts 'generating messages'
  messages = Message.seed

  puts 'setting up cluster'
  cluster = Cluster.new

  puts 'starting producer'
  producer = Producer.new(messages)

  puts 'starting consumer'
  consumer = Consumer.new
rescue Exception
  puts 'stopping'
  cluster.destroy
end
