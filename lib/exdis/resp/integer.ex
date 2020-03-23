defmodule Exdis.RESP.Integer do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    min_value = Exdis.Int64.min()
    max_value = Exdis.Int64.max()
    max_length = Exdis.Int64.max_decimal_string_length()
    line_parser_opts = [max_length: max_length]
    line_parser = Exdis.RESP.Line.parser(line_parser_opts)
    &parse(&1, min_value, max_value, line_parser)
  end

  def encode(value) do
    string = Integer.to_string(value)
    Exdis.RESP.Line.encode(string)
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp parse(data, min_value, max_value, line_parser) do
    case line_parser.(data) do
      {:parsed, line, rest} ->
        handle_parsed_line(line, min_value, max_value, rest)
      {:more, line_parser, rest} ->
        {:more, &parse(&1, min_value, max_value, line_parser), rest}
    end
  end

  defp handle_parsed_line(line, min_value, max_value, rest) do
    case Integer.parse(line) do
      {integer, ""} when integer >= min_value and integer <= max_value ->
        {:parsed, integer, rest};
      {_out_of_range, ""} ->
        raise "FIXME"
      {_, _trailing_data} ->
        raise "FIXME"
      :error ->
        raise "FIXME"
    end
  end
end
