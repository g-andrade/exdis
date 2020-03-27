defmodule Exdis.CommandParsers.String do
  ## ------------------------------------------------------------------
  ## APPEND Command
  ## ------------------------------------------------------------------

  def append([{:string, key_name}, {:string, tail}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.append(&1, tail)}
  end

  def append([_, _]) do
    {:error, :bad_syntax}
  end

  def append(_) do
    {:error, {:wrong_number_of_arguments, :"APPEND"}}
  end

  ## ------------------------------------------------------------------
  ## BITCOUNT command
  ## ------------------------------------------------------------------

  def bit_count([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.bit_count(&1, 0, -1)}
  end

  def bit_count([{:string, key_name}, resp_start, resp_finish]) do
    case {Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_start),
          Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_finish)}
    do
      {{:ok, start}, {:ok, finish}} ->
        {:ok, [key_name], &Exdis.Database.Value.String.bit_count(&1, start, finish)}
      {{:error, _}, _} ->
        {:error, {:not_an_integer_or_out_of_range, :start}}
      {_, {:error, _}} ->
        {:error, {:not_an_integer_or_out_of_range, :end}}
    end
  end

  def bit_count(args) when length(args) in [1, 3] do
    {:error, :bad_syntax}
  end

  def bit_count(_) do
    {:error, {:wrong_number_of_arguments, :"BITCOUNT"}}
  end

  ## ------------------------------------------------------------------
  ## BITPOS command
  ## ------------------------------------------------------------------

  def bit_position([{:string, key_name}, resp_bit]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_bit(resp_bit) do
      {:ok, bit} ->
        {:ok, [key_name], &Exdis.Database.Value.String.bit_position(&1, bit)}
      {:error, _} ->
        {:error, {:not_a_bit, :bit}}
    end
  end

  def bit_position([{:string, key_name}, resp_bit, resp_start]) do
    case {Exdis.CommandParsers.Util.maybe_coerce_into_bit(resp_bit),
          Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_start)}
    do
      {{:ok, bit}, {:ok, start}} ->
        {:ok, [key_name], &Exdis.Database.Value.String.bit_position(&1, bit, start)}
      {{:error, _}, _} ->
        {:error, {:not_a_bit, :bit}}
      {_, {:error, _}} ->
        {:error, {:not_an_integer_or_out_of_range, :start}}
    end
  end

  def bit_position([{:string, key_name}, resp_bit, resp_start, resp_finish]) do
    case {Exdis.CommandParsers.Util.maybe_coerce_into_bit(resp_bit),
          Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_start),
          Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_finish)}
    do
      {{:ok, bit}, {:ok, start}, {:ok, finish}} ->
        {:ok, [key_name], &Exdis.Database.Value.String.bit_position(&1, bit, start, finish)}
      {{:error, _}, _, _} ->
        {:error, {:not_a_bit, :bit}}
      {_, {:error, _}, _} ->
        {:error, {:not_an_integer_or_out_of_range, :start}}
      {_, _, {:error, _}} ->
        {:error, {:not_an_integer_or_out_of_range, :end}}
    end
  end

  def bit_position(args) when length(args) in [2, 3, 4] do
    {:error, :bad_syntax}
  end

  def bit_position(_) do
    {:error, {:wrong_number_of_arguments, :"BITPOS"}}
  end

  ## ------------------------------------------------------------------
  ## DECR Command
  ## ------------------------------------------------------------------

  def decrement([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, -1)}
  end

  def decrement([_]) do
    {:error, :bad_syntax}
  end

  def decrement(_) do
    {:error, {:wrong_number_of_arguments, :"DECR"}}
  end

  ## ------------------------------------------------------------------
  ## DECRBY Command
  ## ------------------------------------------------------------------

  def decrement_by([{:string, key_name}, resp_decrement]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_decrement) do
      {:ok, decrement} ->
        {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, -decrement)}
      {:error, _} ->
        {:error, {:not_an_integer_or_out_of_range, :decrement}}
    end
  end

  def decrement_by([_, _]) do
    {:error, :bad_syntax}
  end

  def decrement_by(_) do
    {:error, {:wrong_number_of_arguments, :"DECRBY"}}
  end

  ## ------------------------------------------------------------------
  ## GET Command
  ## ------------------------------------------------------------------

  def get([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.get(&1)}
  end

  def get([_]) do
    {:error, :bad_syntax}
  end

  def get(_) do
    {:error, {:wrong_number_of_arguments, :"GET"}}
  end

  ## ------------------------------------------------------------------
  ## GETBIT Command
  ## ------------------------------------------------------------------

  def get_bit([{:string, key_name}, resp_offset]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_offset) do
      {:ok, offset} when offset >= 0 ->
        {:ok, [key_name], &Exdis.Database.Value.String.get_bit(&1, offset)}
      _ ->
        {:error, {:not_an_integer_or_out_of_range, :"bit offset"}}
    end
  end

  def get_bit([_, _]) do
    {:error, :bad_syntax}
  end

  def get_bit(_) do
    {:error, {:wrong_number_of_arguments, :"GETBIT"}}
  end

  ## ------------------------------------------------------------------
  ## GETRANGE Command
  ## ------------------------------------------------------------------

  def get_range([{:string, key_name}, resp_start, resp_finish]) do
    case {Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_start),
          Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_finish)}
    do
      {{:ok, start}, {:ok, finish}} ->
        {:ok, [key_name], &Exdis.Database.Value.String.get_range(&1, start, finish)}
      {{:error, _}, _} ->
        {:error, {:not_an_integer_or_out_of_range, :start}}
      {_, {:error, _}} ->
        {:error, {:not_an_integer_or_out_of_range, :end}}
    end
  end

  def get_range([_, _, _]) do
    {:error , :bad_syntax}
  end

  def get_range(_) do
    {:error, {:wrong_number_of_arguments, :"GETRANGE"}}
  end

  ## ------------------------------------------------------------------
  ## GETSET Command
  ## ------------------------------------------------------------------

  def get_set([{:string, key_name}, resp_value]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_string(resp_value) do
      {:ok, value} ->
        {:ok, [key_name], &Exdis.Database.Value.String.get_set(&1, value)}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  def get_set([_, _]) do
    {:error, :bad_syntax}
  end

  def get_set(_) do
    {:error, {:wrong_number_of_arguments, :"GETSET"}}
  end

  ## ------------------------------------------------------------------
  ## INCR Command
  ## ------------------------------------------------------------------

  def increment([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, +1)}
  end

  def increment([_]) do
    {:error, :bad_syntax}
  end

  def increment(_) do
    {:error, {:wrong_number_of_arguments, :"INCR"}}
  end

  ## ------------------------------------------------------------------
  ## INCRBY Command
  ## ------------------------------------------------------------------

  def increment_by([{:string, key_name}, resp_increment]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_int64(resp_increment) do
      {:ok, increment} ->
        {:ok, [key_name], &Exdis.Database.Value.String.increment_by(&1, +increment)}
      {:error, _} ->
        {:error, {:not_an_integer_or_out_of_range, :increment}}
    end
  end

  def increment_by([_, _]) do
    {:error, :bad_syntax}
  end

  def increment_by(_) do
    {:error, {:wrong_number_of_arguments, :"INCRBY"}}
  end

  ## ------------------------------------------------------------------
  ## INCRBYFLOAT Command
  ## ------------------------------------------------------------------

  def increment_by_float([{:string, key_name}, resp_increment]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_float(resp_increment) do
      {:ok, increment} ->
        {:ok, [key_name], &Exdis.Database.Value.String.increment_by_float(&1, +increment)}
      {:error, _} ->
        {:error, {:not_a_valid_float, :increment}}
    end
  end

  def increment_by_float([_, _]) do
    {:error, :bad_syntax}
  end

  def increment_by_float(_) do
    {:error, {:wrong_number_of_arguments, :"INCRBY"}}
  end

  ## ------------------------------------------------------------------
  ## MGET Command
  ## ------------------------------------------------------------------

  def mget(args) do
    case Exdis.CommandParsers.Util.parse_string_list(args, [:non_empty]) do
      {:ok, key_names} ->
        {:ok, key_names, &Exdis.Database.Value.String.mget(&1), [:varargs]}
      {:error, :empty_list} ->
        {:error, {:wrong_number_of_arguments, :"MGET"}}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  ## ------------------------------------------------------------------
  ## MSET Command
  ## ------------------------------------------------------------------

  def mset(args) do
    case Exdis.CommandParsers.Util.parse_and_unzip_kvlist(args, [:non_empty, :unique]) do
      {:ok, key_names, values} ->
        {:ok, key_names, &Exdis.Database.Value.String.mset(&1, values), [:varargs]}
      {:error, :empty_list} ->
        {:error, {:wrong_number_of_arguments, :"MSET"}}
      {:error, {:unpaired_entry, _}} ->
        {:error, {:wrong_number_of_arguments, :"MSET"}}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  ## ------------------------------------------------------------------
  ## SET Command
  ## ------------------------------------------------------------------

  def set([{:string, key_name}, resp_value]) do
    case Exdis.CommandParsers.Util.maybe_coerce_into_string(resp_value) do
      {:ok, value} ->
        {:ok, [key_name], &Exdis.Database.Value.String.set(&1, value)}
      {:error, _} ->
        {:error, :bad_syntax}
    end
  end

  def set([_, _]) do
    {:error, :bad_syntax}
  end

  def set(_) do
    {:error, {:wrong_number_of_arguments, :"SET"}}
  end

  ## ------------------------------------------------------------------
  ## STRLEN Command
  ## ------------------------------------------------------------------

  def str_length([{:string, key_name}]) do
    {:ok, [key_name], &Exdis.Database.Value.String.str_length(&1)}
  end

  def str_length([_]) do
    {:error, :bad_syntax}
  end

  def str_length(_) do
    {:error, {:wrong_number_of_arguments, :"STRLEN"}}
  end
end
