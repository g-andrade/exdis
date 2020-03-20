defmodule Exdis.RESP.BulkString do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @max_length (512 * (1 <<< 20))

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    size_parser = Exdis.RESP.Integer.parser()
    &parse_size(&1, size_parser)
  end

  def encode(iodata) do
    size = :erlang.iolist_size(iodata)
    [
      Exdis.RESP.Integer.encode(size),
      Exdis.RESP.Blob.encode(iodata),
      Exdis.RESP.Line.encode("")
    ]
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp parse_size(data, size_parser) do
    case size_parser.(data) do
      {:parsed, size, rest} when size >= 0 and size <= @max_length ->
        blob_parser = Exdis.RESP.Blob.parser(size)
        parse_blob(rest, blob_parser)
      {:parsed, -1, rest} ->
        # special case
        {:parsed, nil, rest}
      {:parsed, _invalid_size, _rest} ->
        raise "FIXME"
      {:more, size_parser, rest} ->
        {:more, &parse_size(&1, size_parser), rest}
    end
  end

  defp parse_blob(data, blob_parser) do
    case blob_parser.(data) do
      {:parsed, blob, rest} ->
        suffix_parser = Exdis.RESP.Blob.parser(2)
        parse_suffix(rest, blob, suffix_parser)
      {:more, blob_parser, rest} ->
        {:more, &parse_blob(&1, blob_parser), rest}
    end
  end

  defp parse_suffix(data, blob, suffix_parser) do
    case suffix_parser.(data) do
      {:parsed, <<"\r\n">>, rest} ->
        {:parsed, blob, rest}
      {:parsed, _invalid_suffix, _rest} ->
        raise "FIXME"
      {:more, suffix_parser, rest} ->
        {:more, &parse_suffix(&1, blob, suffix_parser), rest}
    end
  end
end
