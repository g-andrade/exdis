defmodule Exdis.Database.Value.Stream do
  require Record

  ## ------------------------------------------------------------------
  ## Records and Type Functions
  ## ------------------------------------------------------------------

  Record.defrecord(:stream,
    consume_callback: nil
  )

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def new(consume_callback) do
    stream(consume_callback: consume_callback)
  end

  def consume(stream(consume_callback: consume_callback) = state) do
    case consume_callback.(nil) do
      {:more, part, consume_callback} ->
        state = stream(state, consume_callback: consume_callback)
        {:more, part, state}
      {:finished, part} ->
        {:finished, part}
    end
  end
end
