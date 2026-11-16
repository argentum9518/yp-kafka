package common

// Создаю тип данных для кодека сообщения
type Message struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Content string `json:"content"`
}

// Создаю тип данных для события блокировки
type BlockEvent struct {
	UserID  string `json:"user_id"`
	Blocked bool   `json:"blocked"`
}

// Создаю тип данных для события "нецензурного" слова
type ForbiddenWordEvent struct {
	Word   string `json:"word"`
	Remove bool   `json:"remove"`
}
