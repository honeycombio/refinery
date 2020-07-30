require "rack"
require "sinatra"
require "zstd-ruby"

# decode the request
before do
  request.body.read.tap do |body|
    Zstd.decompress(body).tap do |data|
      @batch = JSON.parse(data)
    end
  end
end

post "/1/batch/:dataset" do
  # log the payload
  logger.info @batch

  # imitate the response from the honeycomb api
  result = @batch.map do |event|
    { status: 202 }
  end

  JSON.generate result
end
