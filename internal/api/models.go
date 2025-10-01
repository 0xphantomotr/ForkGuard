package api

import "github.com/0xphantomotr/ForkGuard/internal/storage"

type CreateSubscriptionRequest struct {
	ChainID   int64   `json:"chainId"`
	URL       string  `json:"url"`
	MinConfs  int     `json:"minConfs"`
	Address   *string `json:"address,omitempty"`
	Topic0    *string `json:"topic0,omitempty"`
	Topic1    *string `json:"topic1,omitempty"`
	Topic2    *string `json:"topic2,omitempty"`
	Topic3    *string `json:"topic3,omitempty"`
	FromBlock *uint64 `json:"fromBlock,omitempty"`
}

type CreateSubscriptionResponse struct {
	ID        string  `json:"id"`
	Tenant    string  `json:"tenant"`
	ChainID   int64   `json:"chainId"`
	URL       string  `json:"url"`
	Secret    string  `json:"secret"`
	MinConfs  int     `json:"minConfs"`
	Address   *string `json:"address,omitempty"`
	Topic0    *string `json:"topic0,omitempty"`
	Topic1    *string `json:"topic1,omitempty"`
	Topic2    *string `json:"topic2,omitempty"`
	Topic3    *string `json:"topic3,omitempty"`
	FromBlock *uint64 `json:"fromBlock,omitempty"`
	Active    bool    `json:"active"`
}

type UpdateSubscriptionRequest struct {
	URL       *string `json:"url,omitempty"`
	Secret    *string `json:"secret,omitempty"`
	MinConfs  *int    `json:"minConfs,omitempty"`
	Address   *string `json:"address,omitempty"`
	Topic0    *string `json:"topic0,omitempty"`
	Topic1    *string `json:"topic1,omitempty"`
	Topic2    *string `json:"topic2,omitempty"`
	Topic3    *string `json:"topic3,omitempty"`
	FromBlock *uint64 `json:"fromBlock,omitempty"`
	Active    *bool   `json:"active,omitempty"`
}

func (r *CreateSubscriptionRequest) toStorageSubscription(tenant, secret string) *storage.Subscription {
	return &storage.Subscription{
		Tenant:    tenant,
		ChainID:   r.ChainID,
		URL:       r.URL,
		Secret:    secret,
		MinConfs:  r.MinConfs,
		Address:   r.Address,
		Topic0:    r.Topic0,
		Topic1:    r.Topic1,
		Topic2:    r.Topic2,
		Topic3:    r.Topic3,
		FromBlock: r.FromBlock,
		Active:    true,
	}
}

func toCreateSubscriptionResponse(sub *storage.Subscription) *CreateSubscriptionResponse {
	return &CreateSubscriptionResponse{
		ID:        sub.ID,
		Tenant:    sub.Tenant,
		ChainID:   sub.ChainID,
		URL:       sub.URL,
		Secret:    sub.Secret,
		MinConfs:  sub.MinConfs,
		Address:   sub.Address,
		Topic0:    sub.Topic0,
		Topic1:    sub.Topic1,
		Topic2:    sub.Topic2,
		Topic3:    sub.Topic3,
		FromBlock: sub.FromBlock,
		Active:    sub.Active,
	}
}
