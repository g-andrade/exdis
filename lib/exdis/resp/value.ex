defmodule Exdis.RESP.Value do
  ## ------------------------------------------------------------------
  ## Macro-like Attribute Definitions
  ## ------------------------------------------------------------------

  @simple_string ?+
  @error ?-
  @integer ?:
  @bulk_string ?$
  @array ?*

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    &parse_message_type/1
  end

  def encode(value) do
    case value do
      {:simple_string, iodata} ->
        [@simple_string, Exdis.RESP.SimpleString.encode(iodata)]
      {:error, iodata} ->
        [@error, Exdis.RESP.Error.encode(iodata)]
      {:integer, integer} ->
        [@integer, Exdis.RESP.Integer.encode(integer)]
      {:string, iodata} ->
        [@bulk_string, Exdis.RESP.BulkString.encode(iodata)]
      {:array, list} ->
        [@array, Exdis.RESP.Array.encode(list)]
      nil ->
        [@array, Exdis.RESP.Array.encode(nil)]
    end
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp parse_message_type(<<type, rest :: bytes>>) do
    case type do
      @simple_string ->
        parser = Exdis.RESP.SimpleString.parser()
        parse_message_body(:string, rest, parser)
      @error ->
        parser = Exdis.RESP.Error.parser()
        parse_message_body(:error, rest, parser)
      @integer ->
        parser = Exdis.RESP.Integer.parser()
        parse_message_body(:integer, rest, parser)
      @bulk_string ->
        parser = Exdis.RESP.BulkString.parser()
        parse_message_body(:string, rest, parser)
      @array ->
        parser = Exdis.RESP.Array.parser()
        parse_message_body(:array, rest, parser)
      _invalid_type ->
        raise "FIXME"
    end
  end

  defp parse_message_body(tag, data, parser) do
    case parser.(data) do
      {:parsed, message_value, rest} ->
        {:parsed, {tag, message_value}, rest}
      {:more, parser, rest} ->
        {:more, &parse_message_body(tag, &1, parser), rest}
    end
  end
end
