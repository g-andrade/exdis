defmodule Exdis.RESP.Blob do
  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser(fixed_size) do 
    &parse(&1, fixed_size)
  end

  def encode(iodata) do
    iodata
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp parse(data, fixed_size) do
    case data do
      <<blob :: bytes-size(fixed_size), rest :: bytes>> ->
        blob = :binary.copy(blob)
        {:parsed, blob, rest}
      <<_ :: bytes>> ->
        {:more, &parse(&1, fixed_size), data}
    end
  end
end
