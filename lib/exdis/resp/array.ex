defmodule Exdis.RESP.Array do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Public Function Definitions
  ## ------------------------------------------------------------------

  def parser() do
    size_parser = Exdis.RESP.Integer.parser()
    &parse_size(&1, size_parser)
  end

  def encode(list) when is_list(list) do
    size = length(list)
    encoded_size = Exdis.RESP.Integer.encode(size)
    encoded_elements = Enum.map(list, &Exdis.RESP.Value.encode/1)
    [encoded_size | encoded_elements]
  end

  def encode(nil) do
    # special case
    encoded_size = Exdis.RESP.Integer.encode(-1)
    encoded_size
  end

  ## ------------------------------------------------------------------
  ## Private Function Definitions
  ## ------------------------------------------------------------------

  defp parse_size(data, size_parser) do
    case size_parser.(data) do
      {:parsed, size, rest} when size >= 0 ->
        acc = []
        parse_elements(rest, size, acc)
      {:parsed, -1, rest} ->
        # special case
        {:parsed, nil, rest}
      {:parsed, _invalid_size, _rest} ->
        raise "FIXME"
      {:more, size_parser, rest} ->
        {:more, &parse_size(&1, size_parser), rest}
    end
  end

  defp parse_elements(data, elements_left, acc) do
    cond do
      elements_left > 0 ->
        element_parser = Exdis.RESP.Value.parser()
        parse_element(data, elements_left, element_parser, acc)
      elements_left === 0 ->
        elements = Enum.reverse(acc)
        {:parsed, elements, data}
    end
  end

  defp parse_element(data, elements_left, element_parser, acc) do
    case element_parser.(data) do
      {:parsed, element, rest} ->
        elements_left = elements_left - 1
        acc = [element | acc]
        parse_elements(rest, elements_left, acc)
      {:more, element_parser, rest} ->
        {:more, &parse_element(&1, elements_left, element_parser, acc), rest}
    end
  end
end
