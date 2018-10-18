require_relative 'spec_helper'

describe Processor do
  let(:processor) { Processor.new }
  let(:realtime_video_1) { Video.new.tap { |video| video.save } }
  let(:delayed_video_1) { Video.new.tap { |video| video.increment(:video_played, 100); video.save } }
  let(:delayed_video_2) { Video.new.tap { |video| video.increment(:video_played, 100); video.save } }

  context 'process' do
    context 'realtime' do
      it 'updates play count for the video' do
        expect{
          processor.process(Message.new({ video_id: realtime_video_1.id, qty: 1 }))
        }.to change { realtime_video_1.get(:video_played) }.by(1)
      end

      it 'does not buffer play count' do
        expect{
          processor.process(Message.new({ video_id: realtime_video_1.id, qty: 1 }))
        }.to_not change { processor.buffer.size }
      end

      it 'publishes' do
        expect(processor).to receive(:publish).exactly(1).times

        processor.process(Message.new({ video_id: realtime_video_1.id, qty: 1 }))
      end
    end

    context 'delayed' do
      it 'does not update play count for the video' do
        expect{
          processor.process(Message.new({ video_id: delayed_video_1.id, qty: 1 }))
        }.to_not change { delayed_video_1.get(:video_played) }
      end

      it 'buffers play count' do
        expect{
          processor.process(Message.new({ video_id: delayed_video_1.id, qty: 1 }))
        }.to change { processor.buffer.size }.by(1)
      end

      it 'buffers play count for each video' do
        expect{
          processor.process(Message.new({ video_id: delayed_video_1.id, qty: 1 }))
          processor.process(Message.new({ video_id: delayed_video_1.id, qty: 1 }))
          processor.process(Message.new({ video_id: delayed_video_2.id, qty: 1 }))
        }.to change { processor.buffer.size }.by(2)

        expect(processor.buffer[delayed_video_1.id]).to eq(2)
        expect(processor.buffer[delayed_video_2.id]).to eq(1)
      end

      it 'does not publish' do
        expect(processor).to receive(:publish).exactly(0).times

        processor.process(Message.new({ video_id: delayed_video_1.id, qty: 1 }))
      end
    end
  end

  context 'batch_process' do
    before(:each) do
      processor.buffer[delayed_video_1.id] += 10
      processor.buffer[delayed_video_2.id] += 20
    end

    it 'updates play count for all videos' do
      expect {
        expect {
          processor.batch_process
        }.to change { delayed_video_1.get(:video_played) }.by(10)
      }.to change { delayed_video_2.get(:video_played) }.by(20)
    end

    it 'clears buffer' do
      expect {
        processor.batch_process
      }.to change { processor.buffer.size }.to(0)
    end

    it 'publishes' do
      expect(processor).to receive(:publish).exactly(3).times

      50.times do
        video = Video.new.tap { |video| video.save }
        processor.buffer[video.id] = 1
      end

      processor.batch_process
    end
  end
end
