defmodule Exdis.IoData do
  use Bitwise
  require Record

  ## ------------------------------------------------------------------
  ## Record and Type Definitions
  ## ------------------------------------------------------------------

  Record.defrecord(:io_data,
    bytes: nil,
    size: nil,
    fragments: nil
  )

  @opaque t :: record(:io_data,
    bytes: iodata,
    size: non_neg_integer,
    fragments: non_neg_integer)

  ## ------------------------------------------------------------------
  ## API Functions
  ## ------------------------------------------------------------------

  def append(io_data(bytes: bytes, size: size, fragments: fragments) = io_data, tail_bytes) do
    {tail_size, tail_fragments} = count_size_and_fragments(tail_bytes)
    io_data(io_data,
      bytes: [bytes, tail_bytes],
      size: size + tail_size,
      fragments: fragments + tail_fragments)
  end

  def bit_count(io_data, start, finish) do
    range_bytes = get_range(io_data, start, finish)
    bit_count_recur(range_bytes, 0)
  end

  def bit_position(io_data, bit, start, finish) do
    io_data(bytes: bytes, size: size) = io_data

    case normalize_byte_range(size, start, finish) do
      {:valid, start, length} ->
        {_, 0, range_bytes} = get_range_recur(bytes, start, length, [])
        bit_position_recur(range_bytes, bit, start * 8)
      :invalid ->
        {:skipped, size(io_data)}
    end
  end

  def bytes(io_data(bytes: bytes)), do: bytes

  def flatten(io_data(bytes: bytes, size: size) = io_data) do
    binary = :erlang.iolist_to_binary(bytes)
    ^size = byte_size(binary)
    io_data(io_data, bytes: binary, size: size, fragments: 1)
  end

  def fragments(io_data(fragments: fragments)), do: fragments

  # optimization
  def get_bit(io_data(size: size), offset) when offset >= size * 8 do
    0
  end

  def get_bit(io_data(bytes: bytes), offset) when offset >= 0 do
    case find_bit_recur(bytes, offset) do
      {:found, bit_value} ->
        bit_value
      {:skipped, _} ->
        0
    end
  end

  def get_range(io_data(bytes: bytes, size: size), start, finish) do
    case normalize_byte_range(size, start, finish) do
      {:valid, start, length} ->
        {_, 0, range_bytes} = get_range_recur(bytes, start, length, [])
        range_bytes
      :invalid ->
        ""
    end
  end

  def new(bytes) do
    {size, fragments} = count_size_and_fragments(bytes)
    io_data(
      bytes: bytes,
      size: size,
      fragments: fragments)
  end

  def size(io_data(size: size)), do: size

  ## ------------------------------------------------------------------
  ## Private Function: count_size_and_fragments
  ## ------------------------------------------------------------------

  defp count_size_and_fragments(bytes) do
    count_size_and_fragments_recur(bytes, 0, 0)
  end

  defp count_size_and_fragments_recur(<<binary :: bytes>>, size_acc, fragments_acc) do
    {size_acc + byte_size(binary), fragments_acc + 1}
  end

  defp count_size_and_fragments_recur([list_head | list_tail], size_acc, fragments_acc) do
    {size_acc, fragments_acc} = count_size_and_fragments_recur(list_head, size_acc, fragments_acc)
    count_size_and_fragments_recur(list_tail, size_acc, fragments_acc)
  end

  defp count_size_and_fragments_recur([], size_acc, fragments_acc) do
    {size_acc, fragments_acc}
  end

  defp count_size_and_fragments_recur(byte, size_acc, fragments_acc)
  when is_integer(byte) and byte >= 0 and byte < 256
  do
    {size_acc + 1, fragments_acc + 1}
  end

  ## -----------------------------------------------------------------
  ## Private Functions: bit_count_recur
  ## ------------------------------------------------------------------

  defp bit_count_recur(<<binary :: bytes>>, acc) do
    acc + Exdis.Bitstring.bit_count(binary)
  end

  defp bit_count_recur([head|tail], acc) do
    acc = bit_count_recur(head, acc)
    bit_count_recur(tail, acc)
  end

  defp bit_count_recur([], acc), do: acc

  defp bit_count_recur(byte, acc) when is_integer(byte) do
    acc + Exdis.Byte.bit_count(byte)
  end

  ## -----------------------------------------------------------------
  ## Private Functions: bit0_position_recur
  ## ------------------------------------------------------------------

  defp bit_position_recur(<<binary :: bytes>>, bit, acc) do
    case Exdis.Bitstring.bit_position(binary, bit) do
      :skipped ->
        {:skipped, bit_size(binary)}
      {:found, offset} ->
        {:found, acc + offset}
    end
  end

  defp bit_position_recur([head|tail], bit, acc) do
    case bit_position_recur(head, bit, acc) do
      {:skipped, acc} ->
        bit_position_recur(tail, bit, acc)
      {:found, _} = found ->
        found
    end
  end

  defp bit_position_recur([], _bit, acc) do
    {:skipped, acc}
  end

  defp bit_position_recur(byte, bit, acc) when is_integer(byte) do
    case Exdis.Byte.bit_position(byte, bit) do
      {:found, offset} ->
        {:found, acc + offset}
      :skipped ->
        {:skipped, acc + 8}
    end
  end

  ## ------------------------------------------------------------------
  ## Private Functions: count_size_and_fragments
  ## ------------------------------------------------------------------

  defp get_range_recur(_bytes, start, length, chunks_acc) when length === 0 do
    range_bytes = Enum.reverse(chunks_acc)
    {start, length, range_bytes}
  end

  defp get_range_recur(<<binary :: bytes>>, start, length, chunks_acc) do
    binary_size = byte_size(binary)
    case start >= binary_size do
      true ->
        start = start - binary_size
        {start, length, chunks_acc}
      false ->
        chunk_size = min(length, binary_size - start)
        <<_ :: bytes-size(start), chunk :: bytes-size(chunk_size), _ :: bytes>> = binary
        start = 0
        length = length - chunk_size
        chunks_acc = [chunk | chunks_acc]
        {start, length, chunks_acc}
    end
  end

  defp get_range_recur([list_head | list_tail], start, length, chunks_acc) do
    {start, length, chunks_acc} = get_range_recur(list_head, start, length, chunks_acc)
    get_range_recur(list_tail, start, length, chunks_acc)
  end

  defp get_range_recur([], start, length, chunks_acc) do
    {start, length, chunks_acc}
  end

  defp get_range_recur(byte, start, length, chunks_acc) when is_integer(byte) do
    start = start + 1
    length = length - 1
    chunks_acc = [byte | chunks_acc]
    {start, length, chunks_acc}
  end

  ## ------------------------------------------------------------------
  ## Private Function: find_bit_recur
  ## ------------------------------------------------------------------

  defp find_bit_recur(<<binary :: bytes>>, offset) do
    case binary do
      <<_ :: bits-size(offset), bit_value :: 1, _ :: bits>> ->
        # bit value found within binary
        {:found, bit_value}
      _ ->
        {:skipped, offset - bit_size(binary)}
    end
  end

  defp find_bit_recur([list_head | list_tail], offset) do
    case find_bit_recur(list_head, offset) do
      {:found, _} = found ->
        found
      {:skipped, new_offset} ->
        find_bit_recur(list_tail, new_offset)
    end
  end

  defp find_bit_recur([], offset) do
    {:skipped, offset}
  end

  defp find_bit_recur(byte, offset) when is_integer(byte) do
    case offset < 8 do
      true ->
        bit_value = (byte >>> offset) &&& 1
        {:found, bit_value}
      false ->
        {:skipped, 8}
    end
  end

  ## ------------------------------------------------------------------
  ## Private Functions: Normalization of Offsets
  ## ------------------------------------------------------------------

  defp normalize_byte_range(size, start, finish) when finish === nil do
    normalize_byte_range(size, start, -1)
  end

  defp normalize_byte_range(size, start, finish) do
    start = max(0, normalize_byte_offset(size, start))
    finish = min(size - 1, normalize_byte_offset(size, finish))

    case start >= 0 and start < size and start <= finish do
      true ->
        length = finish - start + 1
        {:valid, start, length}
      false ->
        :invalid
    end
  end

  defp normalize_byte_offset(_size, offset) when offset >= 0, do: offset
  defp normalize_byte_offset(size, offset), do: size + offset
end
