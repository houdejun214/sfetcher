package com.sdata.core;

import java.util.List;

public interface FetchDispatch {

	public abstract void dispatch(FetchDatum data);

	public abstract boolean dispatch(List<FetchDatum> list);

	public abstract FetchDatum poll();

	public abstract long getSize();

}