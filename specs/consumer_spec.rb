require 'json'
require_relative './spec_helper'

describe Consumer do
  let(:cluster) { Cluster.new }
  let(:consumer) { Consumer.new }
  let(:producer) { Producer.new(Array(message)) }
  let(:video) { Video.new.tap { |video| video.save } }
  let(:message) { Message.new({ video_id: video.id, qty: 1 }).to_json }

  before(:each) do
    cluster
    consumer
    producer.execute_messages
  end

  after(:each) do
    consumer.worker.terminate
    producer.worker.terminate
    cluster.destroy
  end

  context 'execute_message' do
    it 'processes message' do
      expect(consumer.processor).to receive(:process).exactly(1).times

      consumer.execute_message
    end
  end

  context 'execute_batch_message' do
    it 'processes batch message' do
      expect(consumer.processor).to receive(:batch_process).exactly(1).times

      consumer.execute_batch_message
    end
  end
end
